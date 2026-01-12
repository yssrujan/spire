package cassandrastore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
)

// Attested Nodes Table Schema:
// CREATE TABLE IF NOT EXISTS attested_nodes (
//     spiffe_id text PRIMARY KEY,
//     data_type text,
//     serial_number text,
//     expires_at timestamp,
//     new_serial_number text,
//     new_expires_at timestamp,
//     can_reattest boolean,
//     selectors set<frozen<tuple<text, text>>>,
//     created_at timestamp,
//     updated_at timestamp
// )

// CREATE TABLE IF NOT EXISTS attested_nodes_by_expiry (
//     expiry_bucket text,           -- Daily bucket: 'YYYY-MM-DD'
//     expires_at timestamp,
//     spiffe_id text,
//     data_type text,
//     serial_number text,
//     can_reattest boolean,
//     PRIMARY KEY (expiry_bucket, expires_at, spiffe_id)
// ) WITH CLUSTERING ORDER BY (expires_at ASC, spiffe_id ASC)

// DataStore defines the interface for managing attested nodes
// 	CountAttestedNodes(context.Context, *CountAttestedNodesRequest) (int32, error)
// 	CreateAttestedNode(context.Context, *common.AttestedNode) (*common.AttestedNode, error)
// 	DeleteAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error)
// 	FetchAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error)
// 	ListAttestedNodes(context.Context, *ListAttestedNodesRequest) (*ListAttestedNodesResponse, error)
// 	UpdateAttestedNode(context.Context, *common.AttestedNode, *common.AttestedNodeMask) (*common.AttestedNode, error)
// 	PruneAttestedExpiredNodes(ctx context.Context, expiredBefore time.Time, includeNonReattestable bool) error

// AttestedNode represents an attested node as attested_node_entries table
type AttestedNode struct {
	ID        gocql.UUID
	CreatedAt time.Time
	UpdatedAt time.Time

	SpiffeID        string
	DataType        string
	SerialNumber    string
	ExpiresAt       time.Time
	NewSerialNumber string
	NewExpiresAt    *time.Time
	CanReattest     bool

	Selectors []NodeSelector
}

// createAttestedNode creates a new attested node. Fails if the node already exists.
func createAttestedNode(
	ctx context.Context,
	session *gocql.Session,
	node *common.AttestedNode,
	log logrus.FieldLogger,
) (*common.AttestedNode, error) {

	if node == nil || node.SpiffeId == "" {
		return nil, status.Error(codes.InvalidArgument, "missing spiffe_id")
	}

	now := time.Now().UTC()
	expiresAt := time.Unix(node.CertNotAfter, 0).UTC()
	bucket := expiryBucket(node.CertNotAfter)
	selectorSet := selectorsToSet(node.Selectors)

	// IMPORTANT: MapScanCAS requires a non-nil map
	row := make(map[string]interface{})

	// 1. CAS insert into PRIMARY TABLE ONLY
	applied, err := session.Query(`
		INSERT INTO attested_nodes (
			spiffe_id,
			data_type,
			serial_number,
			expires_at,
			new_serial_number,
			new_expires_at,
			can_reattest,
			selectors,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		IF NOT EXISTS`,
		node.SpiffeId,
		node.AttestationDataType,
		node.CertSerialNumber,
		expiresAt,
		node.NewCertSerialNumber,
		nullableUnixTimeToTime(node.NewCertNotAfter),
		node.CanReattest,
		selectorSet,
		now,
		now,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		MapScanCAS(row)

	if err != nil {
		return nil, fmt.Errorf("failed to insert attested node: %w", err)
	}
	if !applied {
		return nil, status.Error(codes.AlreadyExists, "node already exists")
	}

	// 2. Secondary index insert (NO CAS)
	if err := session.Query(`
		INSERT INTO attested_nodes_by_expiry (
			expiry_bucket,
			expires_at,
			spiffe_id,
			data_type,
			serial_number,
			can_reattest
		) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket,
		expiresAt,
		node.SpiffeId,
		node.AttestationDataType,
		node.CertSerialNumber,
		node.CanReattest,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec(); err != nil {

		log.WithError(err).
			WithField("spiffe_id", node.SpiffeId).
			Warn("Failed to insert attested_nodes_by_expiry index")
	}

	_ = createAttestedNodeEvent(ctx, session, node.SpiffeId)

	return node, nil
}

// fetchAttestedNode retrieves a single attested node by SPIFFE ID.
func fetchAttestedNode(ctx context.Context, session *gocql.Session, spiffeID string) (*common.AttestedNode, error) {
	var (
		dataType        string
		serialNumber    string
		expiresAt       time.Time
		newSerialNumber string
		newExpiresAt    *time.Time
		canReattest     bool
		selectorRaw     [][]string
	)

	query := `
		SELECT data_type, serial_number, expires_at,
		       new_serial_number, new_expires_at, can_reattest, selectors
		FROM attested_nodes
		WHERE spiffe_id = ?`

	err := session.Query(query, spiffeID).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&dataType, &serialNumber, &expiresAt, &newSerialNumber, &newExpiresAt, &canReattest, &selectorRaw)

	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch attested node: %w", err)
	}

	return &common.AttestedNode{
		SpiffeId:            spiffeID,
		AttestationDataType: dataType,
		CertSerialNumber:    serialNumber,
		CertNotAfter:        expiresAt.Unix(),
		NewCertSerialNumber: newSerialNumber,
		NewCertNotAfter:     timeToNullableUnixTime(newExpiresAt),
		CanReattest:         canReattest,
		Selectors:           parseSelectorSet(selectorRaw),
	}, nil
}

// updateAttestedNode updates an existing attested node. Uses last-write-wins semantics.
// Maintains eventual consistency with the expiry index.
func updateAttestedNode(ctx context.Context, session *gocql.Session, node *common.AttestedNode, mask *common.AttestedNodeMask, log logrus.FieldLogger) (*common.AttestedNode, error) {
	// Fetch existing node
	existing, err := fetchAttestedNode(ctx, session, node.SpiffeId)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, status.Error(codes.NotFound, "node not found")
	}

	// Apply default mask if none provided
	if mask == nil {
		mask = &common.AttestedNodeMask{
			CertSerialNumber:    true,
			CertNotAfter:        true,
			NewCertSerialNumber: true,
			NewCertNotAfter:     true,
			CanReattest:         true,
		}
	}

	// Build update statement
	var updates []string
	var args []interface{}

	if mask.CertSerialNumber {
		updates = append(updates, "serial_number = ?")
		args = append(args, node.CertSerialNumber)
	}
	if mask.CertNotAfter {
		updates = append(updates, "expires_at = ?")
		args = append(args, time.Unix(node.CertNotAfter, 0).UTC())
	}
	if mask.NewCertSerialNumber {
		updates = append(updates, "new_serial_number = ?")
		args = append(args, node.NewCertSerialNumber)
	}
	if mask.NewCertNotAfter {
		updates = append(updates, "new_expires_at = ?")
		args = append(args, nullableUnixTimeToTime(node.NewCertNotAfter))
	}
	if mask.CanReattest {
		updates = append(updates, "can_reattest = ?")
		args = append(args, node.CanReattest)
	}

	if len(updates) == 0 {
		return existing, nil // No changes
	}

	updates = append(updates, "updated_at = ?")
	args = append(args, time.Now().UTC())
	args = append(args, node.SpiffeId)

	err = retryTransient(ctx, log, false, func() error {
		batch := session.NewBatch(gocql.LoggedBatch).
			WithContext(ctx)

		// Update main table
		query := fmt.Sprintf("UPDATE attested_nodes SET %s WHERE spiffe_id = ?",
			strings.Join(updates, ", "))
		batch.Query(query, args...)

		// Update expiry index if expires_at changed
		if mask.CertNotAfter && existing.CertNotAfter != node.CertNotAfter {
			oldBucket := expiryBucket(existing.CertNotAfter)
			newBucket := expiryBucket(node.CertNotAfter)
			oldExpiresAt := time.Unix(existing.CertNotAfter, 0).UTC()
			newExpiresAt := time.Unix(node.CertNotAfter, 0).UTC()

			// Delete old expiry index entry
			batch.Query(`
				DELETE FROM attested_nodes_by_expiry
				WHERE expiry_bucket = ? AND expires_at = ? AND spiffe_id = ?`,
				oldBucket, oldExpiresAt, node.SpiffeId)

			// Insert new expiry index entry
			batch.Query(`
				INSERT INTO attested_nodes_by_expiry (
					expiry_bucket, expires_at, spiffe_id, data_type, serial_number, can_reattest
				) VALUES (?, ?, ?, ?, ?, ?)`,
				newBucket, newExpiresAt, node.SpiffeId,
				existing.AttestationDataType,
				node.CertSerialNumber,
				node.CanReattest)
		}

		return session.ExecuteBatch(batch)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to update attested node: %w", err)
	}

	return fetchAttestedNode(ctx, session, node.SpiffeId)
}

// deleteAttestedNode deletes an attested node and its expiry index entry atomically.
func deleteAttestedNode(ctx context.Context, session *gocql.Session, spiffeID string, log logrus.FieldLogger) (*common.AttestedNode, error) {
	// Fetch node before deletion
	node, err := fetchAttestedNode(ctx, session, spiffeID)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, status.Error(codes.NotFound, "node not found")
	}

	bucket := expiryBucket(node.CertNotAfter)
	expiresAt := time.Unix(node.CertNotAfter, 0).UTC()

	err = retryTransient(ctx, log, false, func() error {
		batch := session.NewBatch(gocql.LoggedBatch).
			WithContext(ctx)

		// Delete from main table
		batch.Query(`DELETE FROM attested_nodes WHERE spiffe_id = ?`, spiffeID)

		// Delete from expiry index
		batch.Query(`
			DELETE FROM attested_nodes_by_expiry
			WHERE expiry_bucket = ? AND expires_at = ? AND spiffe_id = ?`,
			bucket, expiresAt, spiffeID)

		return session.ExecuteBatch(batch)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to delete attested node: %w", err)
	}

	if err := createAttestedNodeEvent(ctx, session, spiffeID); err != nil {
		return nil, err
	}

	return node, nil
}

// listAttestedNodes lists attested nodes with pagination and filtering.
func listAttestedNodes(ctx context.Context, session *gocql.Session, req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, error) {
	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "pagesize cannot be zero")
	}

	pageSize := defaultPageSize
	if req.Pagination != nil && req.Pagination.PageSize > 0 {
		pageSize = int(req.Pagination.PageSize)
	}

	// Build query
	query := `SELECT spiffe_id FROM attested_nodes`
	var args []interface{}

	// Token-based pagination
	if req.Pagination != nil && req.Pagination.Token != "" {
		query += ` WHERE token(spiffe_id) > token(?)`
		args = append(args, req.Pagination.Token)
	}

	// Fetch one extra to determine if there are more results
	query += ` LIMIT ?`
	args = append(args, pageSize+1)

	// Execute query
	iter := session.Query(query, args...).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Iter()

	var ids []string
	var id string
	for iter.Scan(&id) {
		ids = append(ids, id)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list attested nodes: %w", err)
	}

	// Determine pagination
	hasMore := len(ids) > pageSize
	var nextToken string
	if hasMore {
		nextToken = ids[pageSize]
		ids = ids[:pageSize]
	}

	// Fetch full node data
	var nodes []*common.AttestedNode
	for _, sid := range ids {
		node, err := fetchAttestedNode(ctx, session, sid)
		if err != nil {
			return nil, err
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	// Apply in-memory filters
	nodes = filterAttestedNodes(nodes, req)

	resp := &datastore.ListAttestedNodesResponse{
		Nodes: nodes,
	}

	if req.Pagination != nil && hasMore {
		resp.Pagination = &datastore.Pagination{
			PageSize: int32(pageSize),
			Token:    nextToken,
		}
	}

	return resp, nil
}

// countAttestedNodes counts attested nodes matching the filter criteria.
func countAttestedNodes(ctx context.Context, session *gocql.Session, req *datastore.CountAttestedNodesRequest) (int32, error) {
	listReq := &datastore.ListAttestedNodesRequest{
		ByAttestationType: req.ByAttestationType,
		ByBanned:          req.ByBanned,
		ByExpiresBefore:   req.ByExpiresBefore,
		BySelectorMatch:   req.BySelectorMatch,
		FetchSelectors:    req.FetchSelectors,
		ByCanReattest:     req.ByCanReattest,
		Pagination: &datastore.Pagination{
			PageSize: 1000,
		},
	}

	var count int32
	for {
		resp, err := listAttestedNodes(ctx, session, listReq)
		if err != nil {
			return 0, err
		}

		count += int32(len(resp.Nodes))

		if resp.Pagination == nil || resp.Pagination.Token == "" {
			break
		}
		listReq.Pagination.Token = resp.Pagination.Token
	}

	return count, nil
}

func pruneAttestedExpiredNodes(ctx context.Context, session *gocql.Session, expiredBefore time.Time, includeNonReattestable bool, log logrus.FieldLogger) error {
	query := `SELECT spiffe_id FROM attested_node_entries WHERE expires_at < ? ALLOW FILTERING`
	iter := session.Query(query, expiredBefore).WithContext(ctx).Iter()

	var spiffeID string
	toDelete := []string{}
	for iter.Scan(&spiffeID) {
		toDelete = append(toDelete, spiffeID)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to list expired nodes: %w", err)
	}

	for _, id := range toDelete {
		node, err := fetchAttestedNode(ctx, session, id)
		if err != nil {
			continue
		}

		if !includeNonReattestable && !node.CanReattest {
			continue
		}

		if node.CertSerialNumber == "" { // Banned
			continue
		}

		if _, err := deleteAttestedNode(ctx, session, id, log); err != nil {
			return err
		}
	}

	return nil
}

func parseSelectorSet(raw [][]string) []*common.Selector {
	result := make([]*common.Selector, 0, len(raw))
	for _, pair := range raw {
		if len(pair) == 2 {
			result = append(result, &common.Selector{Type: pair[0], Value: pair[1]})
		}
	}
	return result
}

func selectorsToSet(selectors []*common.Selector) [][]string {
	set := make([][]string, 0, len(selectors))
	for _, s := range selectors {
		set = append(set, []string{s.Type, s.Value})
	}
	// Sorting is optional but helps with testing/determinism
	sort.Slice(set, func(i, j int) bool {
		if set[i][0] == set[j][0] {
			return set[i][1] < set[j][1]
		}
		return set[i][0] < set[j][0]
	})
	return set
}

// retryTransient with different strategy for logged batches
func retryTransient(ctx context.Context, log logrus.FieldLogger, isLoggedBatch bool, fn func() error) error {
	maxRetries := maxOtherRetries
	if isLoggedBatch {
		maxRetries = maxCreateRetries // be very conservative with logged batch retries
	}

	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := minDuration(initialBackoff*time.Duration(1<<uint(attempt-1)), maxBackoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err = fn()
		if err == nil {
			return nil
		}

		if !isTransient(err) {
			return err
		}

		if log != nil {
			log.WithError(err).
				WithField("attempt", attempt+1).
				WithField("logged_batch", isLoggedBatch).
				Warn("Retrying transient Cassandra operation")
		}
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, err)
}

func isTransient(err error) bool {
	switch err.(type) {
	case *gocql.RequestErrWriteTimeout, *gocql.RequestErrReadTimeout,
		*gocql.RequestErrUnavailable:
		return true
	default:
		return false
	}
}

func nullableUnixTimeToTime(unixTime int64) *time.Time {
	if unixTime == 0 {
		return nil
	}
	t := time.Unix(unixTime, 0)
	return &t
}

func timeToNullableUnixTime(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.Unix()
}

// expiryBucket converts a Unix timestamp to a daily bucket string (YYYY-MM-DD).
func expiryBucket(notAfter int64) string {
	if notAfter <= 0 {
		return "never"
	}
	return time.Unix(notAfter, 0).UTC().Format("2006-01-02")
}

// filterAttestedNodes applies in-memory filters to a list of nodes.
func filterAttestedNodes(
	nodes []*common.AttestedNode,
	req *datastore.ListAttestedNodesRequest,
) []*common.AttestedNode {
	var filtered []*common.AttestedNode

	for _, node := range nodes {
		// Filter by attestation type
		if req.ByAttestationType != "" && node.AttestationDataType != req.ByAttestationType {
			continue
		}

		// Filter by banned status
		if req.ByBanned != nil {
			isBanned := node.CertSerialNumber == ""
			if *req.ByBanned != isBanned {
				continue
			}
		}

		// Filter by expiration
		if !req.ByExpiresBefore.IsZero() {
			if node.CertNotAfter >= req.ByExpiresBefore.Unix() {
				continue
			}
		}

		// Filter by can reattest
		if req.ByCanReattest != nil && *req.ByCanReattest != node.CanReattest {
			continue
		}

		// Filter by selector match
		if req.BySelectorMatch != nil && !matchesSelectors(node, req.BySelectorMatch) {
			continue
		}

		filtered = append(filtered, node)
	}

	return filtered
}

// matchesSelectors checks if a node matches the selector criteria.
func matchesSelectors(node *common.AttestedNode, match *datastore.BySelectors) bool {
	if match == nil {
		return true
	}

	nodeSelectors := make(map[string]map[string]bool)
	for _, sel := range node.Selectors {
		if nodeSelectors[sel.Type] == nil {
			nodeSelectors[sel.Type] = make(map[string]bool)
		}
		nodeSelectors[sel.Type][sel.Value] = true
	}

	switch match.Match {
	case datastore.Exact:
		return exactMatch(nodeSelectors, match.Selectors)
	case datastore.Subset:
		return subsetMatch(nodeSelectors, match.Selectors)
	case datastore.Superset:
		return supersetMatch(nodeSelectors, match.Selectors)
	default:
		return true
	}
}

func exactMatch(nodeSelectors map[string]map[string]bool, required []*common.Selector) bool {
	if len(nodeSelectors) != len(countUniqueTypes(required)) {
		return false
	}
	for _, req := range required {
		if !nodeSelectors[req.Type][req.Value] {
			return false
		}
	}
	return true
}

func subsetMatch(nodeSelectors map[string]map[string]bool, required []*common.Selector) bool {
	for _, req := range required {
		if !nodeSelectors[req.Type][req.Value] {
			return false
		}
	}
	return true
}

func supersetMatch(nodeSelectors map[string]map[string]bool, required []*common.Selector) bool {
	requiredMap := make(map[string]map[string]bool)
	for _, req := range required {
		if requiredMap[req.Type] == nil {
			requiredMap[req.Type] = make(map[string]bool)
		}
		requiredMap[req.Type][req.Value] = true
	}

	for typ, values := range requiredMap {
		nodeValues := nodeSelectors[typ]
		for val := range values {
			if !nodeValues[val] {
				return false
			}
		}
	}
	return true
}

func countUniqueTypes(selectors []*common.Selector) map[string]bool {
	types := make(map[string]bool)
	for _, s := range selectors {
		types[s.Type] = true
	}
	return types
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
