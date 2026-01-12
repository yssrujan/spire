package cassandrastore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Table Schema for Node Selectors:
// CREATE TABLE IF NOT EXISTS node_selectors (
//     spiffe_id text,
//     selector_type text,
//     selector_value text,
//     created_at timestamp,
//     PRIMARY KEY (spiffe_id, selector_type, selector_value)
// )

// CREATE TABLE IF NOT EXISTS node_selectors_by_selector (
//     selector_type text,
//     selector_value text,
//     spiffe_id text,
//     PRIMARY KEY ((selector_type, selector_value), spiffe_id)
// ) WITH CLUSTERING ORDER BY (spiffe_id ASC)

// DataStore defines the interface for managing node selectors
// 		GetNodeSelectors(ctx context.Context, spiffeID string, consistency datastore.DataConsistency) ([]*common.Selector, error)
// 		SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) error
// 		ListNodeSelectors(ctx context.Context, req *ListNodeSelectorsRequest) (*ListNodeSelectorsResponse, error)

// NodeSelector represents a node selector in the attested_nodes table
type NodeSelector struct {
	SpiffeID string
	Type     string
	Value    string
}

// getNodeSelectors retrieves selectors for an attested node by SPIFFE ID.
// Returns NotFound if the node does not exist.
func getNodeSelectors(
	ctx context.Context,
	session *gocql.Session,
	spiffeID string,
	consistency datastore.DataConsistency,
) ([]*common.Selector, error) {

	if spiffeID == "" {
		return nil, status.Error(codes.InvalidArgument, "SPIFFE ID is required")
	}

	var raw [][]string

	err := session.Query(`
		SELECT selectors
		FROM attested_nodes
		WHERE spiffe_id = ?`,
		spiffeID,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Scan(&raw)

	if err == gocql.ErrNotFound {
		return nil, status.Error(codes.NotFound, "attested node not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch node selectors: %w", err)
	}

	return parseSelectorTuples(raw), nil
}

// setNodeSelectors replaces selectors for an existing attested node.
func setNodeSelectors(
	ctx context.Context,
	session *gocql.Session,
	spiffeID string,
	selectors []*common.Selector,
	log logrus.FieldLogger,
) error {
	log.WithField("spiffe_id", spiffeID).Info("SETTING NODE SELECTORS")
	if spiffeID == "" {
		return status.Error(codes.InvalidArgument, "SPIFFE ID is required")
	}

	for _, sel := range selectors {
		if sel.Type == "" || sel.Value == "" {
			return status.Error(codes.InvalidArgument, "selector type and value cannot be empty")
		}
	}

	selectorSet := selectorsToTuples(selectors)
	now := time.Now().UTC()

	// Ensure node exists (SPIRE requires hard failure if not)
	var exists string
	err := session.Query(`
		SELECT spiffe_id
		FROM attested_nodes
		WHERE spiffe_id = ?
	`, spiffeID).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&exists)

	if err == gocql.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to verify attested node: %w", err)
	}

	// Single-row update, no batch
	if err := session.Query(`
		UPDATE attested_nodes
		SET selectors = ?, updated_at = ?
		WHERE spiffe_id = ?
	`, selectorSet, now, spiffeID).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec(); err != nil {
		return fmt.Errorf("failed to update node selectors: %w", err)
	}

	return nil
}

// listNodeSelectors lists selectors for all attested nodes.
func listNodeSelectors(
	ctx context.Context,
	session *gocql.Session,
	req *datastore.ListNodeSelectorsRequest,
) (*datastore.ListNodeSelectorsResponse, error) {

	iter := session.Query(`
		SELECT spiffe_id, selectors, expires_at
		FROM attested_nodes`,
	).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		PageSize(1000).
		Iter()

	result := make(map[string][]*common.Selector)

	var (
		spiffeID  string
		raw       [][]string
		expiresAt time.Time
	)

	for iter.Scan(&spiffeID, &raw, &expiresAt) {
		if !req.ValidAt.IsZero() {
			if !expiresAt.IsZero() && expiresAt.Before(req.ValidAt) {
				continue
			}
		}
		result[spiffeID] = parseSelectorTuples(raw)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list node selectors: %w", err)
	}

	return &datastore.ListNodeSelectorsResponse{
		Selectors: result,
	}, nil
}

// parseSelectorTuples converts raw tuple format to common.Selector objects.
func parseSelectorTuples(raw [][]string) []*common.Selector {
	out := make([]*common.Selector, 0, len(raw))
	for _, pair := range raw {
		if len(pair) == 2 {
			out = append(out, &common.Selector{
				Type:  pair[0],
				Value: pair[1],
			})
		}
	}
	return out
}

// selectorsToTuples converts common.Selector objects to tuple format.
// Sorts for determinism and consistency.
func selectorsToTuples(selectors []*common.Selector) [][]string {
	out := make([][]string, 0, len(selectors))
	for _, s := range selectors {
		out = append(out, []string{s.Type, s.Value})
	}

	// Sort for determinism - important for Cassandra set comparison
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] == out[j][0] {
			return out[i][1] < out[j][1]
		}
		return out[i][0] < out[j][0]
	})

	return out
}
