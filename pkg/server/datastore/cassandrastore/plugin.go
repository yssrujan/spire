package cassandrastore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/hcl"
	"github.com/sirupsen/logrus"
	configv1 "github.com/spiffe/spire-plugin-sdk/proto/spire/service/common/config/v1"
	"github.com/spiffe/spire/pkg/common/catalog"
)

const (
	PluginName               = "cassandra"
	defaultPageSize          = 100
	defaultConnectTimeout    = 15 * time.Second
	defaultQueryTimeout      = 30 * time.Second
	defaultReplicationFactor = 3
)

type configuration struct {
	Hosts             []string `hcl:"hosts" json:"hosts"`
	Port              int      `hcl:"port" json:"port"`
	Keyspace          string   `hcl:"keyspace" json:"keyspace"`
	Username          string   `hcl:"username" json:"username"`
	Password          string   `hcl:"password" json:"password"`
	Consistency       string   `hcl:"consistency" json:"consistency"`
	ReplicationFactor int      `hcl:"replication_factor" json:"replication_factor"`
	ConnectTimeout    string   `hcl:"connect_timeout" json:"connect_timeout"`
	QueryTimeout      string   `hcl:"query_timeout" json:"query_timeout"`
}

type Plugin struct {
	session             *gocql.Session
	log                 logrus.FieldLogger
	useServerTimestamps bool
}

func New(log logrus.FieldLogger) *Plugin {
	return &Plugin{
		log: log,
	}
}

func (cfg *configuration) Validate() error {
	if len(cfg.Hosts) == 0 {
		return fmt.Errorf("hosts configuration is required")
	}
	if cfg.Keyspace == "" {
		return fmt.Errorf("keyspace configuration is required")
	}
	if strings.ContainsAny(cfg.Keyspace, " ;") {
		return fmt.Errorf("invalid keyspace name")
	}
	if cfg.Port <= 0 {
		return fmt.Errorf("invalid port: %d", cfg.Port)
	}
	if cfg.ReplicationFactor <= 0 {
		return fmt.Errorf("replication_factor must be >= 1")
	}
	return nil
}

func (ds *Plugin) Close() error {
	if ds.session != nil {
		ds.session.Close()
		ds.session = nil
	}
	return nil
}

func (ds *Plugin) Configure(ctx context.Context, hclConfiguration string) (err error) {
	// Default configuration
	config := &configuration{
		Port:              9042,
		Consistency:       "QUORUM",
		ReplicationFactor: defaultReplicationFactor,
	}

	if err := hcl.Decode(config, hclConfiguration); err != nil {
		return fmt.Errorf("failed to decode configuration: %w", err)
	}

	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	consistency, err := parseConsistency(config.Consistency)
	if err != nil {
		return fmt.Errorf("invalid consistency level: %w", err)
	}
	connectTimeout := defaultConnectTimeout
	queryTimeout := defaultQueryTimeout

	if config.ConnectTimeout != "" {
		connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
		if err != nil {
			return fmt.Errorf("invalid connect_timeout: %w", err)
		}
	}
	if config.QueryTimeout != "" {
		queryTimeout, err = time.ParseDuration(config.QueryTimeout)
		if err != nil {
			return fmt.Errorf("invalid query_timeout: %w", err)
		}
	}

	cluster := gocql.NewCluster(config.Hosts...)
	cluster.Port = config.Port
	cluster.Consistency = consistency
	cluster.Timeout = queryTimeout
	cluster.ConnectTimeout = connectTimeout
	cluster.PageSize = defaultPageSize
	cluster.NumConns = 2
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 5,
		Min:        200 * time.Millisecond,
		Max:        5 * time.Second,
	}

	// Retry policy: exponential backoff is more resilient than simple retries
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 5,
		Min:        100 * time.Millisecond,
		Max:        10 * time.Second,
	}

	if config.Username != "" || config.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	// connection and create keyspace with proper replication
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to connect to Cassandra cluster: %w", err)
	}
	defer session.Close()

	keyspaceQuery := fmt.Sprintf(`
CREATE KEYSPACE IF NOT EXISTS %s
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': %d
} AND durable_writes = true`,
		config.Keyspace,
		config.ReplicationFactor,
	)
	if err := session.Query(keyspaceQuery).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to create keyspace %s: %w", config.Keyspace, err)
	}

	ds.log.WithFields(logrus.Fields{
		"keyspace":           config.Keyspace,
		"hosts":              config.Hosts,
		"replication_factor": config.ReplicationFactor,
	}).Info("Cassandra keyspace ensured")

	cluster.Keyspace = config.Keyspace

	newSession, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to connect to keyspace %s: %w", config.Keyspace, err)
	}

	// Initialize schema
	// if err := ds.initializeSchema(ctx, newSession); err != nil {
	// 	newSession.Close()
	// 	return fmt.Errorf("failed to initialize schema: %w", err)
	// }

	// Close existing session if reconfiguring
	if ds.session != nil {
		ds.session.Close()
		ds.session = nil
	}

	ds.session = newSession
	ds.log.Info("Successfully configured Cassandra datastore plugin")

	return nil
}

func (ds *Plugin) Validate(ctx context.Context, coreConfig catalog.CoreConfig, configurationstring string) (*configv1.ValidateResponse, error) {
	config := &configuration{}
	if err := hcl.Decode(config, configurationstring); err != nil {
		return &configv1.ValidateResponse{Valid: false}, fmt.Errorf("failed to decode config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return &configv1.ValidateResponse{Valid: false}, err
	}
	return &configv1.ValidateResponse{Valid: true}, nil
}

// Helper to parse consistency levels safely
func parseConsistency(s string) (gocql.Consistency, error) {
	switch s {
	case "ANY":
		return gocql.Any, nil
	case "ONE":
		return gocql.One, nil
	case "TWO":
		return gocql.Two, nil
	case "THREE":
		return gocql.Three, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "ALL":
		return gocql.All, nil
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "EACH_QUORUM":
		return gocql.EachQuorum, nil
	case "LOCAL_ONE":
		return gocql.LocalOne, nil
	default:
		return gocql.Quorum, fmt.Errorf("unknown consistency level: %s", s)
	}
}
