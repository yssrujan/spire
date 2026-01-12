package cassandrastore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// Bundles Table Schema
// CREATE TABLE IF NOT EXISTS bundles (
// 		trust_domain text,
// 		data blob,
// 		created_at timestamp,
// 		updated_at timestamp,
// 		PRIMARY KEY (trust_domain)
// )

// Datastore Bundle Methods
//  AppendBundle(context.Context, *common.Bundle) (*common.Bundle, error)
// 	CountBundles(context.Context) (int32, error)
// 	CreateBundle(context.Context, *common.Bundle) (*common.Bundle, error)
// 	DeleteBundle(ctx context.Context, trustDomainID string, mode DeleteMode) error
// 	FetchBundle(ctx context.Context, trustDomainID string) (*common.Bundle, error)
//	ListBundles(context.Context, *ListBundlesRequest) (*ListBundlesResponse, error)
//	PruneBundle(ctx context.Context, trustDomainID string, expiresBefore time.Time) (changed bool, err error)
//	SetBundle(context.Context, *common.Bundle) (*common.Bundle, error)
//	UpdateBundle(context.Context, *common.Bundle, *common.BundleMask) (*common.Bundle, error)

// Bundle represents a bundle as stored in the bundles table.
type Bundle struct {
	TrustDomain string
	Data        []byte

	CreatedAt time.Time
	UpdatedAt time.Time
}

// appendBundle appends the given bundle to the existing bundle in the datastore.
// This operation is idempotent and uses optimistic concurrency control.
func appendBundle(
	ctx context.Context,
	session *gocql.Session,
	b *common.Bundle,
	log logrus.FieldLogger,
) (*common.Bundle, error) {
	if err := validateBundle(b); err != nil {
		return nil, err
	}

	newModel, err := bundleToModel(b)
	if err != nil {
		return nil, err
	}

	trustDomain := newModel.TrustDomain

	for attempt := 0; attempt < maxCASRetries; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			time.Sleep(backoff)

			if log != nil {
				log.WithFields(logrus.Fields{
					"trust_domain": trustDomain,
					"attempt":      attempt + 1,
				}).Debug("Retrying appendBundle after CAS failure")
			}
		}

		// Fetch existing bundle
		existing := &Bundle{}
		err := session.Query(`
			SELECT trust_domain, data, created_at, updated_at
			FROM bundles
			WHERE trust_domain = ?
		`, trustDomain).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			Scan(
				&existing.TrustDomain,
				&existing.Data,
				&existing.CreatedAt,
				&existing.UpdatedAt,
			)

		// First insert
		if err == gocql.ErrNotFound {
			now := time.Now()
			applied, err := session.Query(`
				INSERT INTO bundles (trust_domain, data, created_at, updated_at)
				VALUES (?, ?, ?, ?)
				IF NOT EXISTS
			`, trustDomain, newModel.Data, now, now).
				WithContext(ctx).
				Consistency(gocql.Quorum).
				MapScanCAS(make(map[string]interface{}))

			if err != nil {
				return nil, fmt.Errorf("cassandra insert error: %w", err)
			}

			if applied {
				return b, nil
			}

			// Race: someone inserted concurrently → retry
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("cassandra read error: %w", err)
		}

		// Parse existing bundle
		existingBundle, err := modelToBundle(existing)
		if err != nil {
			return nil, err
		}

		// Merge
		mergedBundle, changed := bundleutil.MergeBundles(existingBundle, b)
		if !changed {
			return mergedBundle, nil
		}

		mergedBundle.SequenceNumber++

		updatedModel, err := bundleToModel(mergedBundle)
		if err != nil {
			return nil, err
		}

		now := time.Now()

		// Value-based CAS (LEGAL in Cassandra)
		applied, err := session.Query(`
			UPDATE bundles
			SET data = ?, updated_at = ?
			WHERE trust_domain = ?
			IF data = ?
		`, updatedModel.Data, now, trustDomain, existing.Data).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			MapScanCAS(make(map[string]interface{}))

		if err != nil {
			return nil, fmt.Errorf("cassandra update error: %w", err)
		}

		if !applied {
			if log != nil {
				log.WithField("trust_domain", trustDomain).
					Debug("CAS failed, bundle modified concurrently")
			}
			continue
		}

		return mergedBundle, nil
	}

	return nil, status.Errorf(
		codes.Aborted,
		"failed to append bundle after %d attempts due to concurrent updates",
		maxCASRetries,
	)
}

// countBundles counts all bundles in the datastore.
// For large datasets, this can be expensive.
func countBundles(ctx context.Context, session *gocql.Session) (int32, error) {
	var count int32
	err := session.Query(`SELECT COUNT(*) FROM bundles`).
		WithContext(ctx).
		Consistency(gocql.One).
		Scan(&count)

	if err != nil {
		iter := session.Query(`SELECT trust_domain FROM bundles`).
			WithContext(ctx).
			Consistency(gocql.One).
			Iter()

		count = 0
		var trustDomain string
		for iter.Scan(&trustDomain) {
			count++
		}
		if err := iter.Close(); err != nil {
			return 0, newCassandraError("failed to count bundles: %s", err)
		}
	}

	return count, nil
}

// createBundle creates a new bundle in the datastore.
func createBundle(ctx context.Context, session *gocql.Session, bundle *common.Bundle) (*common.Bundle, error) {
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}

	// Create new bundle
	model, err := bundleToModel(bundle)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	model.CreatedAt = now
	model.UpdatedAt = now

	// Insert using LWT (IF NOT EXISTS) to ensure we don't overwrite
	applied, err := session.Query(`
        INSERT INTO bundles (trust_domain, data, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        IF NOT EXISTS
    `, model.TrustDomain, model.Data, model.CreatedAt, model.UpdatedAt).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		MapScanCAS(make(map[string]interface{}))

	if err != nil {
		return nil, newCassandraError("failed to create bundle: %s", err)
	}

	if !applied {
		return nil, status.Error(codes.AlreadyExists, "bundle already exists")
	}

	return bundle, nil
}

// deleteBundle deletes the specified bundle from the datastore.
// Handles federated entries based on the delete mode.
func deleteBundle(ctx context.Context, session *gocql.Session, trustDomainID string, mode datastore.DeleteMode) error {
	if trustDomainID == "" {
		return status.Error(codes.InvalidArgument, "missing trust domain ID")
	}

	// Check if bundle exists
	bundle, err := fetchBundle(ctx, session, trustDomainID)
	if err != nil {
		return err
	}
	if bundle == nil {
		return status.Error(codes.NotFound, "bundle not found")
	}

	// Check for associated registration entries
	count, err := countEntriesFederatedWith(ctx, session, trustDomainID)
	if err != nil {
		return err
	}

	if count > 0 {
		switch mode {
		case datastore.Delete:
			// Delete all associated registration entries
			if err := deleteEntriesFederatedWith(ctx, session, trustDomainID); err != nil {
				return err
			}
		case datastore.Dissociate:
			// Remove federation relationship from entries
			if err := dissociateEntriesFromBundle(ctx, session, trustDomainID); err != nil {
				return err
			}
		case datastore.Restrict:
			return status.Errorf(codes.FailedPrecondition,
				"cannot delete bundle; federated with %d registration entries", count)
		}
	}

	// Delete the bundle with IF EXISTS for idempotency
	_, err = session.Query(
		`DELETE FROM bundles WHERE trust_domain = ? IF EXISTS`,
		trustDomainID,
	).WithContext(ctx).
		Consistency(gocql.Quorum).
		MapScanCAS(make(map[string]interface{}))

	if err != nil {
		return newCassandraError("failed to delete bundle: %s", err)
	}

	return nil
}

// fetchBundle fetches the specified bundle from the datastore.
func fetchBundle(ctx context.Context, session *gocql.Session, trustDomainID string) (*common.Bundle, error) {
	if trustDomainID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing trust domain ID")
	}

	model := &Bundle{}
	if err := session.Query(
		`SELECT trust_domain, data, created_at, updated_at FROM bundles WHERE trust_domain = ?`,
		trustDomainID,
	).WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&model.TrustDomain, &model.Data, &model.CreatedAt, &model.UpdatedAt); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, newCassandraError("failed to fetch bundle: %s", err)
	}

	return modelToBundle(model)
}

// listBundles lists bundles in the datastore with optional pagination.
// Uses token-based pagination for efficient, scalable iteration.
func listBundles(ctx context.Context, session *gocql.Session, req *datastore.ListBundlesRequest) (*datastore.ListBundlesResponse, error) {
	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "page size cannot be zero")
	}

	pageSize := 0
	if req.Pagination != nil && req.Pagination.PageSize > 0 {
		pageSize = int(req.Pagination.PageSize)
	}

	query := "SELECT token(trust_domain), trust_domain, data FROM bundles"
	args := []interface{}{}

	if req.Pagination != nil && req.Pagination.Token != "" {
		tokenVal, err := strconv.ParseInt(req.Pagination.Token, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
		}
		query += " WHERE token(trust_domain) > ?"
		args = append(args, tokenVal)
	}

	// Fetch one extra record to detect if there's a next page
	if pageSize > 0 {
		query += " LIMIT ?"
		args = append(args, pageSize+1)
	}

	iter := session.Query(query, args...).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var bundles []*common.Bundle
	var tokens []int64
	var token int64
	var trustDomain string
	var data []byte

	for iter.Scan(&token, &trustDomain, &data) {
		bundle := new(common.Bundle)
		if err := proto.Unmarshal(data, bundle); err != nil {
			continue
		}
		if bundle.TrustDomainId == "" {
			bundle.TrustDomainId = trustDomain
		}

		bundles = append(bundles, bundle)
		tokens = append(tokens, token)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to iterate bundles: %w", err)
	}

	resp := &datastore.ListBundlesResponse{
		Bundles: bundles,
	}

	if req.Pagination != nil && pageSize > 0 {
		resp.Pagination = &datastore.Pagination{
			PageSize: int32(pageSize),
		}

		// Only set next token if we fetched more than requested (i.e., there is a next page)
		if len(bundles) > pageSize {
			resp.Bundles = bundles[:pageSize]
			resp.Pagination.Token = strconv.FormatInt(tokens[pageSize-1], 10)
		}
	}

	return resp, nil
}

// pruneBundle prunes expired certificates and keys from the specified bundle.
// Uses CAS to prevent race conditions during pruning.
func pruneBundle(ctx context.Context, session *gocql.Session, trustDomainID string, expiresBefore time.Time, log logrus.FieldLogger) (bool, error) {
	if trustDomainID == "" {
		return false, status.Error(codes.InvalidArgument, "missing trust domain ID")
	}

	// Retry loop for CAS operations
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			time.Sleep(backoff)
		}

		// Fetch existing bundle with WRITETIME
		var trustDomain string
		var data []byte
		var createdAt, updatedAt time.Time
		var writeTime int64

		err := session.Query(`
			SELECT trust_domain, data, created_at, updated_at, WRITETIME(data)
			FROM bundles WHERE trust_domain = ?
		`, trustDomainID).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			Scan(&trustDomain, &data, &createdAt, &updatedAt, &writeTime)

		if err != nil {
			if err == gocql.ErrNotFound {
				return false, nil
			}
			return false, fmt.Errorf("unable to fetch current bundle: %w", err)
		}

		// Unmarshal and prune
		currentBundle := new(common.Bundle)
		if err := proto.Unmarshal(data, currentBundle); err != nil {
			return false, fmt.Errorf("failed to unmarshal bundle: %w", err)
		}

		newBundle, changed, err := bundleutil.PruneBundle(currentBundle, expiresBefore, log)
		if err != nil {
			return false, fmt.Errorf("prune failed: %w", err)
		}

		if !changed {
			return false, nil
		}

		// Increment sequence number
		newBundle.SequenceNumber++

		// Marshal updated bundle
		newModel, err := bundleToModel(newBundle)
		if err != nil {
			return false, fmt.Errorf("failed to marshal pruned bundle: %w", err)
		}

		// Update with CAS
		now := time.Now()
		applied, err := session.Query(`
			UPDATE bundles
			SET data = ?, updated_at = ?
			WHERE trust_domain = ?
			IF WRITETIME(data) = ?
		`, newModel.Data, now, trustDomainID, writeTime).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			MapScanCAS(make(map[string]interface{}))

		if err != nil {
			return false, newCassandraError("failed to update pruned bundle: %s", err)
		}

		if !applied {
			if log != nil {
				log.WithField("trust_domain", trustDomainID).Debug("CAS failed during prune, retrying")
			}
			continue
		}

		return true, nil
	}

	return false, status.Errorf(codes.Aborted, "failed to prune bundle after %d attempts", maxCASRetries)
}

// setBundle sets the given bundle in the datastore.
// Unlike a blind upsert, it merges with the existing bundle to preserve appended roots/JWT keys.
func setBundle(ctx context.Context, session *gocql.Session, bundle *common.Bundle) (*common.Bundle, error) {
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}

	trustDomain := bundle.TrustDomainId

	for attempt := 0; attempt < maxCASRetries; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff << attempt
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			time.Sleep(backoff)
		}

		// Fetch current bundle
		current, err := fetchBundle(ctx, session, trustDomain)
		if err != nil {
			return nil, err
		}

		var merged *common.Bundle
		if current == nil {
			// No existing bundle → just use the new one
			merged = bundle
		} else {
			// Merge new bundle into existing (preserves appended certs/keys)
			var changed bool
			merged, changed = bundleutil.MergeBundles(current, bundle)
			if changed {
				merged.SequenceNumber++
			}
		}

		// Marshal and write with simple INSERT (Cassandra upsert semantics)
		newModel, err := bundleToModel(merged)
		if err != nil {
			return nil, err
		}
		now := time.Now()

		if err := session.Query(`
			INSERT INTO bundles (trust_domain, data, created_at, updated_at)
			VALUES (?, ?, ?, ?)
		`, trustDomain, newModel.Data, now, now).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			Exec(); err != nil {
			return nil, fmt.Errorf("failed to set bundle: %w", err)
		}

		return merged, nil
	}

	return nil, status.Errorf(codes.Aborted, "failed to set bundle after %d retries", maxCASRetries)
}

// updateBundle updates the given bundle in the datastore.
// Uses CAS to prevent concurrent modification issues.
func updateBundle(ctx context.Context, session *gocql.Session, newBundle *common.Bundle, mask *common.BundleMask) (*common.Bundle, error) {
	if err := validateBundle(newBundle); err != nil {
		return nil, err
	}

	newModel, err := bundleToModel(newBundle)
	if err != nil {
		return nil, err
	}

	trustDomain := newModel.TrustDomain

	// Retry loop for CAS
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			time.Sleep(backoff)
		}

		// Fetch existing with WRITETIME
		var data []byte
		var createdAt, updatedAt time.Time
		var writeTime int64

		err = session.Query(`
			SELECT data, created_at, updated_at, WRITETIME(data)
			FROM bundles WHERE trust_domain = ?
		`, trustDomain).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			Scan(&data, &createdAt, &updatedAt, &writeTime)

		if err != nil {
			if err == gocql.ErrNotFound {
				return nil, status.Error(codes.NotFound, "bundle not found")
			}
			return nil, newCassandraError("failed to fetch bundle: %s", err)
		}

		model := &Bundle{
			TrustDomain: trustDomain,
			Data:        data,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		}

		// Apply mask
		updatedData, resultBundle, err := applyBundleMask(model, newBundle, mask)
		if err != nil {
			return nil, err
		}

		// Update with CAS
		now := time.Now()
		applied, err := session.Query(`
			UPDATE bundles
			SET data = ?, updated_at = ?
			WHERE trust_domain = ?
			IF WRITETIME(data) = ?
		`, updatedData, now, trustDomain, writeTime).
			WithContext(ctx).
			Consistency(gocql.Quorum).
			MapScanCAS(make(map[string]interface{}))

		if err != nil {
			return nil, newCassandraError("failed to update bundle: %s", err)
		}

		if !applied {
			continue
		}

		return resultBundle, nil
	}

	return nil, status.Errorf(codes.Aborted, "failed to update bundle after %d attempts", maxCASRetries)
}

// Helper functions for bundles

// validateBundle performs basic validation on the given bundle.
func validateBundle(bundle *common.Bundle) error {
	if bundle == nil {
		return status.Error(codes.InvalidArgument, "missing bundle")
	}
	if bundle.TrustDomainId == "" {
		return status.Error(codes.InvalidArgument, "missing trust domain ID")
	}
	return nil
}

// applyBundleMask applies the given mask to the newBundle, merging it with the existing
func applyBundleMask(model *Bundle, newBundle *common.Bundle, inputMask *common.BundleMask) ([]byte, *common.Bundle, error) {
	bundle, err := modelToBundle(model)
	if err != nil {
		return nil, nil, err
	}

	if inputMask == nil {
		inputMask = protoutil.AllTrueCommonBundleMask
	}

	if inputMask.RefreshHint {
		bundle.RefreshHint = newBundle.RefreshHint
	}

	if inputMask.RootCas {
		bundle.RootCas = newBundle.RootCas
	}

	if inputMask.JwtSigningKeys {
		bundle.JwtSigningKeys = newBundle.JwtSigningKeys
	}

	if inputMask.SequenceNumber {
		bundle.SequenceNumber = newBundle.SequenceNumber
	}

	newModel, err := bundleToModel(bundle)
	if err != nil {
		return nil, nil, err
	}

	return newModel.Data, bundle, nil
}

// modelToBundle converts the given bundle model to a Protobuf bundle message. It will also
// include any embedded CACert models.
func modelToBundle(model *Bundle) (*common.Bundle, error) {
	if model == nil {
		return nil, nil
	}
	bundle := new(common.Bundle)
	if err := proto.Unmarshal(model.Data, bundle); err != nil {
		return nil, newWrappedCassandraError(err)
	}

	return bundle, nil
}

// bundleToModel converts the given Protobuf bundle message to a database model. It
// performs validation, and fully parses certificates to form CACert embedded models.
func bundleToModel(pb *common.Bundle) (*Bundle, error) {
	if pb == nil {
		return nil, newCassandraError("missing bundle in request")
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return nil, newWrappedCassandraError(err)
	}

	return &Bundle{
		TrustDomain: pb.TrustDomainId,
		Data:        data,
	}, nil
}

// Helper functions for federation management

// countEntriesFederatedWith counts registration entries federated with a trust domain.
func countEntriesFederatedWith(ctx context.Context, session *gocql.Session, trustDomain string) (int32, error) {
	// Use the entries_by_federated_td table from our schema
	var count int32
	err := session.Query(
		`SELECT COUNT(*) FROM entries_by_federated_td WHERE trust_domain = ?`,
		trustDomain,
	).WithContext(ctx).
		Consistency(gocql.One).
		Scan(&count)

	if err != nil {
		// Fallback to iteration if COUNT(*) fails
		iter := session.Query(
			`SELECT entry_id FROM entries_by_federated_td WHERE trust_domain = ?`,
			trustDomain,
		).WithContext(ctx).
			Consistency(gocql.One).
			Iter()

		count = 0
		var entryID string
		for iter.Scan(&entryID) {
			count++
		}
		if err := iter.Close(); err != nil {
			return 0, fmt.Errorf("failed to count federated entries: %w", err)
		}
	}

	return count, nil
}

// deleteEntriesFederatedWith deletes all entries federated with a trust domain.
func deleteEntriesFederatedWith(ctx context.Context, session *gocql.Session, trustDomain string) error {
	// Collect entry IDs
	iter := session.Query(
		`SELECT entry_id FROM entries_by_federated_td WHERE trust_domain = ?`,
		trustDomain,
	).WithContext(ctx).Consistency(gocql.Quorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to list federated entries: %w", err)
	}

	// Delete each entry (this will be implemented in entries.go)
	for _, id := range entryIDs {
		if _, err := deleteRegistrationEntry(ctx, session, id); err != nil {
			return fmt.Errorf("failed to delete entry %s: %w", id, err)
		}
	}

	return nil
}

// dissociateEntriesFromBundle removes federation relationship between entries and a trust domain.
func dissociateEntriesFromBundle(ctx context.Context, session *gocql.Session, trustDomain string) error {
	// Collect entry IDs
	iter := session.Query(
		`SELECT entry_id FROM entries_by_federated_td WHERE trust_domain = ?`,
		trustDomain,
	).WithContext(ctx).Consistency(gocql.Quorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to list federated entries: %w", err)
	}

	// Remove federation from both tables
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.SetConsistency(gocql.Quorum)

	for _, id := range entryIDs {
		batch.Query(
			`DELETE FROM entries_federated_with WHERE entry_id = ? AND trust_domain = ?`,
			id, trustDomain,
		)
		batch.Query(
			`DELETE FROM entries_by_federated_td WHERE trust_domain = ? AND entry_id = ?`,
			trustDomain, id,
		)
	}

	if err := session.ExecuteBatch(batch.WithContext(ctx)); err != nil {
		return fmt.Errorf("failed to dissociate entries: %w", err)
	}

	return nil
}
