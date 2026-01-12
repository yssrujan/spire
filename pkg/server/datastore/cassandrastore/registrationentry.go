package cassandrastore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/gofrs/uuid/v5"
	"github.com/gogo/status"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
)

// CREATE TABLE IF NOT EXISTS registered_entries (
//     entry_id text PRIMARY KEY,
//     spiffe_id text,
//     parent_id text,
//     ttl int,                      -- X.509 SVID TTL in seconds
//     jwt_svid_ttl int,
//     admin boolean,
//     downstream boolean,
//     expiry bigint,                -- Unix timestamp, 0 = no expiry
//     revision_number bigint,       -- Incremented on each update
//     store_svid boolean,
//     hint text,
//     selectors set<frozen<tuple<text, text>>>,  -- Set of (type, value) pairs
//     dns_names set<text>,
//     federated_trust_domains set<text>,
//     created_at timestamp,
//     updated_at timestamp
// )

// CREATE TABLE IF NOT EXISTS registered_entries_by_spiffe (
//     spiffe_id text,
//     entry_id text,
//     parent_id text,
//     selectors set<frozen<tuple<text, text>>>,
//     created_at timestamp,
//     PRIMARY KEY (spiffe_id, entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// CREATE TABLE IF NOT EXISTS registered_entries_by_parent (
//     parent_id text,
//     entry_id text,
//     spiffe_id text,
//     selectors set<frozen<tuple<text, text>>>,
//     created_at timestamp,
//     PRIMARY KEY (parent_id, entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// CREATE TABLE IF NOT EXISTS registered_entries_by_selector (
//     selector_type text,
//     selector_value text,
//     entry_id text,
//     spiffe_id text,
//     parent_id text,
//     all_selectors set<frozen<tuple<text, text>>>,  -- Full selector set for filtering
//     PRIMARY KEY ((selector_type, selector_value), entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// CREATE TABLE IF NOT EXISTS registered_entries_by_hint (
//     hint text,
//     entry_id text,
//     spiffe_id text,
//     PRIMARY KEY (hint, entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// CREATE TABLE IF NOT EXISTS registered_entries_by_expiry (
//     expiry_bucket text,
//     expiry bigint,
//     entry_id text,
//     spiffe_id text,
//     PRIMARY KEY (expiry_bucket, expiry, entry_id)
// ) WITH CLUSTERING ORDER BY (expiry ASC, entry_id ASC)

// CREATE TABLE IF NOT EXISTS registered_entries_by_downstream (
//     downstream boolean,
//     entry_id text,
//     spiffe_id text,
//     parent_id text,
//     PRIMARY KEY (downstream, entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// Datastore interface methods for registration entries:
// 		CreateRegistrationEntry(context.Context, *common.RegistrationEntry) (*common.RegistrationEntry, error)
// 		CreateOrReturnRegistrationEntry(context.Context, *common.RegistrationEntry) (*common.RegistrationEntry, bool, error)
// 		CountRegistrationEntries(context.Context, *CountRegistrationEntriesRequest) (int32, error)
// 		DeleteRegistrationEntry(ctx context.Context, entryID string) (*common.RegistrationEntry, error)
// 		FetchRegistrationEntry(ctx context.Context, entryID string) (*common.RegistrationEntry, error)
// 		FetchRegistrationEntries(ctx context.Context, entryIDs []string) (map[string]*common.RegistrationEntry, error)
// 		ListRegistrationEntries(context.Context, *ListRegistrationEntriesRequest) (*ListRegistrationEntriesResponse, error)
// 		PruneRegistrationEntries(ctx context.Context, expiresBefore time.Time) error
// 		UpdateRegistrationEntry(context.Context, *common.RegistrationEntry, *common.RegistrationEntryMask) (*common.RegistrationEntry, error)

// 		GetNodeSelectors(ctx context.Context, spiffeID string, dataConsistency DataConsistency) ([]*common.Selector, error)
// 		ListNodeSelectors(context.Context, *ListNodeSelectorsRequest) (*ListNodeSelectorsResponse, error)
// 		SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) error

// RegisteredEntry represents a registration entry
type RegisteredEntry struct {
	ID        gocql.UUID
	CreatedAt time.Time
	UpdatedAt time.Time

	EntryID        string
	SpiffeID       string
	ParentID       string
	TTL            int32
	Admin          bool
	Downstream     bool
	Expiry         int64
	StoreSvid      bool
	JwtSvidTTL     int32
	Hint           string
	RevisionNumber int64

	Selectors     []Selector
	DNSList       []DNSName
	FederatesWith []string
}

// Selector represents a selector
type Selector struct {
	ID        gocql.UUID
	CreatedAt time.Time
	UpdatedAt time.Time

	RegisteredEntryID string
	Type              string
	Value             string
}

// DNSName represents a DNS name as dns_names table
type DNSName struct {
	ID                gocql.UUID
	RegisteredEntryID string

	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

const maxBatchSize = 50 // Maximum items per batch to avoid large batches

// createRegistrationEntry creates a new registration entry
func createRegistrationEntry(ctx context.Context, session *gocql.Session, entry *common.RegistrationEntry, log logrus.FieldLogger) (*common.RegistrationEntry, error) {
	result, _, err := createOrReturnRegistrationEntry(ctx, session, entry, log)
	return result, err
}

// createOrReturnRegistrationEntry creates a new registration entry or returns an existing similar one
func createOrReturnRegistrationEntry(ctx context.Context, session *gocql.Session, entry *common.RegistrationEntry, log logrus.FieldLogger) (*common.RegistrationEntry, bool, error) {
	if err := validateRegistrationEntry(entry); err != nil {
		return nil, false, err
	}

	// Check for similar entry (same parent, spiffe_id, and selectors)
	existing, err := lookupSimilarEntry(ctx, session, entry)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return existing, true, nil
	}

	// Generate entry ID if not provided
	entryID := entry.EntryId
	if entryID == "" {
		u, err := uuid.NewV4()
		if err != nil {
			return nil, false, fmt.Errorf("failed to generate entry ID: %w", err)
		}
		entryID = u.String()
	}

	now := time.Now()
	expiryBucket := getExpiryBucket(entry.EntryExpiry)

	// Convert selectors and dns_names to sets
	selectorSet := make([]string, 0, len(entry.Selectors))
	for _, s := range entry.Selectors {
		selectorSet = append(selectorSet, fmt.Sprintf("%s:%s", s.Type, s.Value))
	}

	// Use batch for atomic write across tables
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.SetConsistency(gocql.Quorum)

	// 1. Insert into primary table (registered_entries)
	batch.Query(`
		INSERT INTO registered_entries (
			entry_id, spiffe_id, parent_id, ttl, jwt_svid_ttl, admin, downstream,
			expiry, revision_number, store_svid, hint, selectors, dns_names,
			federated_trust_domains, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, entryID, entry.SpiffeId, entry.ParentId, entry.X509SvidTtl, entry.JwtSvidTtl,
		entry.Admin, entry.Downstream, entry.EntryExpiry, int64(0), entry.StoreSvid,
		entry.Hint, selectorSet, entry.DnsNames, entry.FederatesWith, now, now)

	// 2. Insert into registered_entries_by_spiffe
	batch.Query(`
		INSERT INTO registered_entries_by_spiffe (
			spiffe_id, entry_id, parent_id, selectors, created_at
		) VALUES (?, ?, ?, ?, ?)
	`, entry.SpiffeId, entryID, entry.ParentId, selectorSet, now)

	// 3. Insert into registered_entries_by_parent
	batch.Query(`
		INSERT INTO registered_entries_by_parent (
			parent_id, entry_id, spiffe_id, selectors, created_at
		) VALUES (?, ?, ?, ?, ?)
	`, entry.ParentId, entryID, entry.SpiffeId, selectorSet, now)

	// 4. Insert into registered_entries_by_selector (one row per selector)
	for _, selector := range entry.Selectors {
		batch.Query(`
			INSERT INTO registered_entries_by_selector (
				selector_type, selector_value, entry_id, spiffe_id, parent_id, all_selectors
			) VALUES (?, ?, ?, ?, ?, ?)
		`, selector.Type, selector.Value, entryID, entry.SpiffeId, entry.ParentId, selectorSet)
	}

	// 5. Insert into registered_entries_by_hint (if hint exists)
	if entry.Hint != "" {
		batch.Query(`
			INSERT INTO registered_entries_by_hint (
				hint, entry_id, spiffe_id
			) VALUES (?, ?, ?)
		`, entry.Hint, entryID, entry.SpiffeId)
	}

	// 6. Insert into registered_entries_by_expiry (if has expiry)
	if entry.EntryExpiry > 0 {
		batch.Query(`
			INSERT INTO registered_entries_by_expiry (
				expiry_bucket, expiry, entry_id, spiffe_id
			) VALUES (?, ?, ?, ?)
		`, expiryBucket, entry.EntryExpiry, entryID, entry.SpiffeId)
	}

	// 7. Insert into registered_entries_by_downstream
	batch.Query(`
		INSERT INTO registered_entries_by_downstream (
			downstream, entry_id, spiffe_id, parent_id
		) VALUES (?, ?, ?, ?)
	`, entry.Downstream, entryID, entry.SpiffeId, entry.ParentId)

	// 8. Insert federation relationships
	for _, trustDomain := range entry.FederatesWith {
		batch.Query(`
			INSERT INTO entries_federated_with (entry_id, trust_domain, created_at)
			VALUES (?, ?, ?)
		`, entryID, trustDomain, now)

		batch.Query(`
			INSERT INTO entries_by_federated_td (trust_domain, entry_id, spiffe_id)
			VALUES (?, ?, ?)
		`, trustDomain, entryID, entry.SpiffeId)
	}

	// Execute batch
	if err := session.ExecuteBatch(batch.WithContext(ctx)); err != nil {
		return nil, false, fmt.Errorf("failed to create entry: %w", err)
	}

	// Create event (separate from batch for reliability)
	if err := createRegistrationEntryEvent(ctx, session, entryID); err != nil {
		if log != nil {
			log.WithError(err).Warn("Failed to create registration entry event")
		}
	}

	entry.EntryId = entryID
	entry.CreatedAt = now.Unix()
	entry.RevisionNumber = 0

	return entry, false, nil
}

// countRegistrationEntries counts registration entries based on filters
func countRegistrationEntries(ctx context.Context, session *gocql.Session, req *datastore.CountRegistrationEntriesRequest) (int32, error) {
	listReq := &datastore.ListRegistrationEntriesRequest{
		ByParentID:      req.ByParentID,
		BySpiffeID:      req.BySpiffeID,
		BySelectors:     req.BySelectors,
		ByFederatesWith: req.ByFederatesWith,
		ByHint:          req.ByHint,
		ByDownstream:    req.ByDownstream,
		Pagination: &datastore.Pagination{
			PageSize: 1000,
		},
	}

	count := int32(0)
	for {
		resp, err := listRegistrationEntries(ctx, session, listReq)
		if err != nil {
			return 0, err
		}

		count += int32(len(resp.Entries))

		if resp.Pagination == nil || resp.Pagination.Token == "" {
			break
		}

		listReq.Pagination.Token = resp.Pagination.Token
	}

	return count, nil
}

// deleteRegistrationEntry deletes a registration entry by ID
func deleteRegistrationEntry(ctx context.Context, session *gocql.Session, entryID string) (*common.RegistrationEntry, error) {
	// Fetch entry before deletion
	entry, err := fetchRegistrationEntry(ctx, session, entryID)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, status.Error(codes.NotFound, "entry not found")
	}

	// Use batch for atomic deletion
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.SetConsistency(gocql.Quorum)

	// 1. Delete from primary table
	batch.Query(`DELETE FROM registered_entries WHERE entry_id = ?`, entryID)

	// 2. Delete from by_spiffe
	batch.Query(`
		DELETE FROM registered_entries_by_spiffe
		WHERE spiffe_id = ? AND entry_id = ?
	`, entry.SpiffeId, entryID)

	// 3. Delete from by_parent
	batch.Query(`
		DELETE FROM registered_entries_by_parent
		WHERE parent_id = ? AND entry_id = ?
	`, entry.ParentId, entryID)

	// 4. Delete from by_selector
	for _, selector := range entry.Selectors {
		batch.Query(`
			DELETE FROM registered_entries_by_selector
			WHERE selector_type = ? AND selector_value = ? AND entry_id = ?
		`, selector.Type, selector.Value, entryID)
	}

	// 5. Delete from by_hint
	if entry.Hint != "" {
		batch.Query(`
			DELETE FROM registered_entries_by_hint
			WHERE hint = ? AND entry_id = ?
		`, entry.Hint, entryID)
	}

	// 6. Delete from by_expiry
	if entry.EntryExpiry > 0 {
		bucket := getExpiryBucket(entry.EntryExpiry)
		batch.Query(`
			DELETE FROM registered_entries_by_expiry
			WHERE expiry_bucket = ? AND expiry = ? AND entry_id = ?
		`, bucket, entry.EntryExpiry, entryID)
	}

	// 7. Delete from by_downstream
	batch.Query(`
		DELETE FROM registered_entries_by_downstream
		WHERE downstream = ? AND entry_id = ?
	`, entry.Downstream, entryID)

	// 8. Delete federations
	for _, td := range entry.FederatesWith {
		batch.Query(`
			DELETE FROM entries_federated_with
			WHERE entry_id = ? AND trust_domain = ?
		`, entryID, td)
		batch.Query(`
			DELETE FROM entries_by_federated_td
			WHERE trust_domain = ? AND entry_id = ?
		`, td, entryID)
	}

	// Execute batch
	if err := session.ExecuteBatch(batch.WithContext(ctx)); err != nil {
		return nil, fmt.Errorf("failed to delete entry: %w", err)
	}

	// Create event
	if err := createRegistrationEntryEvent(ctx, session, entryID); err != nil {
		// Don't fail deletion if event creation fails
	}

	return entry, nil
}

// fetchRegistrationEntry fetches a registration entry by ID
func fetchRegistrationEntry(ctx context.Context, session *gocql.Session, entryID string) (*common.RegistrationEntry, error) {
	entries, err := fetchRegistrationEntries(ctx, session, []string{entryID})
	if err != nil {
		return nil, err
	}
	return entries[entryID], nil
}

// fetchRegistrationEntries fetches multiple registration entries by their IDs
func fetchRegistrationEntries(ctx context.Context, session *gocql.Session, entryIDs []string) (map[string]*common.RegistrationEntry, error) {
	result := make(map[string]*common.RegistrationEntry)

	for _, entryID := range entryIDs {
		var (
			spiffeID              string
			parentID              string
			ttl                   int32
			jwtSvidTTL            int32
			admin                 bool
			downstream            bool
			expiry                int64
			revisionNumber        int64
			storeSvid             bool
			hint                  string
			selectorSet           []string
			dnsNames              []string
			federatedTrustDomains []string
			createdAt             time.Time
			updatedAt             time.Time
		)

		query := `
			SELECT spiffe_id, parent_id, ttl, jwt_svid_ttl, admin, downstream, expiry,
				revision_number, store_svid, hint, selectors, dns_names,
				federated_trust_domains, created_at, updated_at
			FROM registered_entries WHERE entry_id = ?
		`

		if err := session.Query(query, entryID).
			WithContext(ctx).
			Consistency(gocql.LocalQuorum).
			Scan(
				&spiffeID, &parentID, &ttl, &jwtSvidTTL, &admin, &downstream, &expiry,
				&revisionNumber, &storeSvid, &hint, &selectorSet, &dnsNames,
				&federatedTrustDomains, &createdAt, &updatedAt,
			); err != nil {
			if err == gocql.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("failed to fetch entry %s: %w", entryID, err)
		}

		// Parse selectors from set
		selectors := make([]*common.Selector, 0, len(selectorSet))
		for _, s := range selectorSet {
			parts := strings.SplitN(s, ":", 2)
			if len(parts) == 2 {
				selectors = append(selectors, &common.Selector{
					Type:  parts[0],
					Value: parts[1],
				})
			}
		}

		entry := &common.RegistrationEntry{
			EntryId:        entryID,
			SpiffeId:       spiffeID,
			ParentId:       parentID,
			X509SvidTtl:    ttl,
			JwtSvidTtl:     jwtSvidTTL,
			Admin:          admin,
			Downstream:     downstream,
			EntryExpiry:    expiry,
			RevisionNumber: revisionNumber,
			StoreSvid:      storeSvid,
			Hint:           hint,
			Selectors:      selectors,
			DnsNames:       dnsNames,
			FederatesWith:  federatedTrustDomains,
			CreatedAt:      createdAt.Unix(),
		}

		result[entryID] = entry
	}

	return result, nil
}

// updateRegistrationEntry updates an existing registration entry
func updateRegistrationEntry(ctx context.Context, session *gocql.Session, entry *common.RegistrationEntry, mask *common.RegistrationEntryMask, log logrus.FieldLogger) (*common.RegistrationEntry, error) {
	if err := validateRegistrationEntryForUpdate(entry, mask); err != nil {
		return nil, err
	}

	entryID := entry.EntryId

	// Retry loop for CAS
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			time.Sleep(backoff)

			if log != nil {
				log.WithFields(logrus.Fields{
					"entry_id": entryID,
					"attempt":  attempt + 1,
				}).Debug("Retrying updateRegistrationEntry after CAS failure")
			}
		}

		// Fetch existing with WRITETIME
		existing, writeTime, err := fetchRegistrationEntryWithWriteTime(ctx, session, entryID)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			return nil, status.Error(codes.NotFound, "entry not found")
		}

		// Build updated entry by applying mask
		updated := applyUpdateMask(existing, entry, mask)
		updated.RevisionNumber = existing.RevisionNumber + 1

		now := time.Now()

		// Determine what needs updating
		needsSelectorUpdate := mask == nil || mask.Selectors
		needsDNSUpdate := mask == nil || mask.DnsNames
		needsFederationUpdate := mask == nil || mask.FederatesWith

		// Prepare batch
		batch := session.NewBatch(gocql.LoggedBatch)
		batch.SetConsistency(gocql.Quorum)

		// Convert updated data to sets
		selectorSet := selectorsToSet(updated.Selectors)

		updateFields := buildUpdateFields(updated, mask)
		batch.Query(fmt.Sprintf(`
			UPDATE registered_entries SET %s
			WHERE entry_id = ?
			IF WRITETIME(spiffe_id) = ?
		`, updateFields),
			append(buildUpdateValues(updated, mask, now), entryID, writeTime)...)

		if needsSelectorUpdate || needsDNSUpdate || needsFederationUpdate {
			// Delete old entries from denormalized tables
			if needsSelectorUpdate {
				for _, selector := range existing.Selectors {
					batch.Query(`
						DELETE FROM registered_entries_by_selector
						WHERE selector_type = ? AND selector_value = ? AND entry_id = ?
					`, selector.Type, selector.Value, entryID)
				}

				// Insert new selectors
				for _, selector := range updated.Selectors {
					batch.Query(`
						INSERT INTO registered_entries_by_selector (
							selector_type, selector_value, entry_id, spiffe_id, parent_id, all_selectors
						) VALUES (?, ?, ?, ?, ?, ?)
					`, selector.Type, selector.Value, entryID, updated.SpiffeId, updated.ParentId, selectorSet)
				}
			}

			// Update by_spiffe and by_parent (always update these as they contain selectors)
			batch.Query(`
				UPDATE registered_entries_by_spiffe SET selectors = ?
				WHERE spiffe_id = ? AND entry_id = ?
			`, selectorSet, updated.SpiffeId, entryID)

			batch.Query(`
				UPDATE registered_entries_by_parent SET selectors = ?
				WHERE parent_id = ? AND entry_id = ?
			`, selectorSet, updated.ParentId, entryID)

			// Update hint index
			if existing.Hint != updated.Hint {
				if existing.Hint != "" {
					batch.Query(`
						DELETE FROM registered_entries_by_hint
						WHERE hint = ? AND entry_id = ?
					`, existing.Hint, entryID)
				}
				if updated.Hint != "" {
					batch.Query(`
						INSERT INTO registered_entries_by_hint (hint, entry_id, spiffe_id)
						VALUES (?, ?, ?)
					`, updated.Hint, entryID, updated.SpiffeId)
				}
			}

			// Update expiry index
			if existing.EntryExpiry != updated.EntryExpiry {
				if existing.EntryExpiry > 0 {
					oldBucket := getExpiryBucket(existing.EntryExpiry)
					batch.Query(`
						DELETE FROM registered_entries_by_expiry
						WHERE expiry_bucket = ? AND expiry = ? AND entry_id = ?
					`, oldBucket, existing.EntryExpiry, entryID)
				}
				if updated.EntryExpiry > 0 {
					newBucket := getExpiryBucket(updated.EntryExpiry)
					batch.Query(`
						INSERT INTO registered_entries_by_expiry (
							expiry_bucket, expiry, entry_id, spiffe_id
						) VALUES (?, ?, ?, ?)
					`, newBucket, updated.EntryExpiry, entryID, updated.SpiffeId)
				}
			}

			// Update downstream index
			if existing.Downstream != updated.Downstream {
				batch.Query(`
					DELETE FROM registered_entries_by_downstream
					WHERE downstream = ? AND entry_id = ?
				`, existing.Downstream, entryID)

				batch.Query(`
					INSERT INTO registered_entries_by_downstream (
						downstream, entry_id, spiffe_id, parent_id
					) VALUES (?, ?, ?, ?)
				`, updated.Downstream, entryID, updated.SpiffeId, updated.ParentId)
			}

			// Update federation
			if needsFederationUpdate {
				// Remove old federations
				for _, td := range existing.FederatesWith {
					batch.Query(`
						DELETE FROM entries_federated_with
						WHERE entry_id = ? AND trust_domain = ?
					`, entryID, td)
					batch.Query(`
						DELETE FROM entries_by_federated_td
						WHERE trust_domain = ? AND entry_id = ?
					`, td, entryID)
				}

				// Add new federations
				for _, td := range updated.FederatesWith {
					batch.Query(`
						INSERT INTO entries_federated_with (entry_id, trust_domain, created_at)
						VALUES (?, ?, ?)
					`, entryID, td, now)
					batch.Query(`
						INSERT INTO entries_by_federated_td (trust_domain, entry_id, spiffe_id)
						VALUES (?, ?, ?)
					`, td, entryID, updated.SpiffeId)
				}
			}
		}

		// Execute batch
		if err := session.ExecuteBatch(batch.WithContext(ctx)); err != nil {
			// Check if CAS failed
			if strings.Contains(err.Error(), "[applied]") {
				// CAS failed, retry
				if log != nil {
					log.WithField("entry_id", entryID).Debug("CAS failed during update")
				}
				continue
			}
			return nil, fmt.Errorf("failed to update entry: %w", err)
		}

		// Create event
		if err := createRegistrationEntryEvent(ctx, session, entryID); err != nil {
			if log != nil {
				log.WithError(err).Warn("Failed to create registration entry event")
			}
		}

		return updated, nil
	}

	return nil, status.Errorf(codes.Aborted, "failed to update entry after %d attempts", maxCASRetries)
}

// listRegistrationEntries lists registration entries based on filters and pagination
func listRegistrationEntries(ctx context.Context, session *gocql.Session, req *datastore.ListRegistrationEntriesRequest) (*datastore.ListRegistrationEntriesResponse, error) {
	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	pageSize := int32(defaultPageSize)
	if req.Pagination != nil && req.Pagination.PageSize > 0 {
		pageSize = req.Pagination.PageSize
	}

	// Determine which table to query based on filters
	var entryIDs []string
	var err error

	switch {
	case req.BySpiffeID != "":
		entryIDs, err = listBySpiffeID(ctx, session, req, pageSize)
	case req.ByParentID != "":
		entryIDs, err = listByParentID(ctx, session, req, pageSize)
	case req.BySelectors != nil && len(req.BySelectors.Selectors) > 0:
		entryIDs, err = listBySelectors(ctx, session, req.BySelectors, pageSize)
	case req.ByHint != "":
		entryIDs, err = listByHint(ctx, session, req.ByHint, pageSize)
	case req.ByFederatesWith != nil && len(req.ByFederatesWith.TrustDomains) > 0:
		entryIDs, err = listByFederatesWith(ctx, session, req.ByFederatesWith, pageSize)
	default:
		entryIDs, err = listAllEntries(ctx, session, req.Pagination, pageSize)
	}

	if err != nil {
		return nil, err
	}

	// Fetch full entries
	entryMap, err := fetchRegistrationEntries(ctx, session, entryIDs)
	if err != nil {
		return nil, err
	}

	// Convert to slice maintaining order
	entries := make([]*common.RegistrationEntry, 0, len(entryIDs))
	for _, id := range entryIDs {
		if entry := entryMap[id]; entry != nil {
			entries = append(entries, entry)
		}
	}

	// Apply client-side filters
	entries = applyClientSideFilters(entries, req)

	// Handle pagination
	resp := &datastore.ListRegistrationEntriesResponse{
		Entries: entries,
	}

	if req.Pagination != nil {
		resp.Pagination = &datastore.Pagination{
			PageSize: pageSize,
		}

		if len(entries) > int(pageSize) {
			resp.Entries = entries[:pageSize]
			resp.Pagination.Token = entries[pageSize-1].EntryId
		} else if len(entries) == int(pageSize) && len(entryIDs) > len(entries) {
			// Might have more
			resp.Pagination.Token = entries[len(entries)-1].EntryId
		}
	}

	return resp, nil
}

// pruneRegistrationEntries deletes registration entries that have expired before the given time
func pruneRegistrationEntries(ctx context.Context, session *gocql.Session, expiresBefore time.Time, log logrus.FieldLogger) error {
	// Query by expiry buckets
	bucket := getExpiryBucket(expiresBefore.Unix())

	// Get all entries that expire before the threshold
	iter := session.Query(`
		SELECT entry_id FROM registered_entries_by_expiry
		WHERE expiry_bucket = ? AND expiry < ?
	`, bucket, expiresBefore.Unix()).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to list expired entries: %w", err)
	}

	// Delete in batches
	for i := 0; i < len(entryIDs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(entryIDs) {
			end = len(entryIDs)
		}

		batchIDs := entryIDs[i:end]
		for _, id := range batchIDs {
			if _, err := deleteRegistrationEntry(ctx, session, id); err != nil {
				if log != nil {
					log.WithError(err).WithField("entry_id", id).Warn("Failed to delete expired entry")
				}
				continue
			}
		}
	}

	if log != nil {
		log.WithField("count", len(entryIDs)).Info("Pruned expired registration entries")
	}

	return nil
}

// Helpers

// buildUpdateFields builds the update fields for the CQL update statement
func buildUpdateFields(entry *common.RegistrationEntry, mask *common.RegistrationEntryMask) string {
	fields := []string{"updated_at = ?"}
	if mask == nil || mask.SpiffeId {
		fields = append(fields, "spiffe_id = ?")
	}
	if mask == nil || mask.ParentId {
		fields = append(fields, "parent_id = ?")
	}
	if mask == nil || mask.X509SvidTtl {
		fields = append(fields, "ttl = ?")
	}
	if mask == nil || mask.JwtSvidTtl {
		fields = append(fields, "jwt_svid_ttl = ?")
	}
	if mask == nil || mask.Admin {
		fields = append(fields, "admin = ?")
	}
	if mask == nil || mask.Downstream {
		fields = append(fields, "downstream = ?")
	}
	if mask == nil || mask.EntryExpiry {
		fields = append(fields, "expiry = ?")
	}
	fields = append(fields, "revision_number = ?") // Always update revision
	if mask == nil || mask.StoreSvid {
		fields = append(fields, "store_svid = ?")
	}
	if mask == nil || mask.Hint {
		fields = append(fields, "hint = ?")
	}
	if mask == nil || mask.Selectors {
		fields = append(fields, "selectors = ?")
	}
	if mask == nil || mask.DnsNames {
		fields = append(fields, "dns_names = ?")
	}
	if mask == nil || mask.FederatesWith {
		fields = append(fields, "federated_trust_domains = ?")
	}
	return strings.Join(fields, ", ")
}

// buildUpdateValues builds the update values for the CQL update statement
func buildUpdateValues(entry *common.RegistrationEntry, mask *common.RegistrationEntryMask, now time.Time) []interface{} {
	values := []interface{}{now}
	if mask == nil || mask.SpiffeId {
		values = append(values, entry.SpiffeId)
	}
	if mask == nil || mask.ParentId {
		values = append(values, entry.ParentId)
	}
	if mask == nil || mask.X509SvidTtl {
		values = append(values, entry.X509SvidTtl)
	}
	if mask == nil || mask.JwtSvidTtl {
		values = append(values, entry.JwtSvidTtl)
	}
	if mask == nil || mask.Admin {
		values = append(values, entry.Admin)
	}
	if mask == nil || mask.Downstream {
		values = append(values, entry.Downstream)
	}
	if mask == nil || mask.EntryExpiry {
		values = append(values, entry.EntryExpiry)
	}
	values = append(values, entry.RevisionNumber)
	if mask == nil || mask.StoreSvid {
		values = append(values, entry.StoreSvid)
	}
	if mask == nil || mask.Hint {
		values = append(values, entry.Hint)
	}
	if mask == nil || mask.Selectors {
		values = append(values, selectorsToSet(entry.Selectors))
	}
	if mask == nil || mask.DnsNames {
		values = append(values, entry.DnsNames)
	}
	if mask == nil || mask.FederatesWith {
		values = append(values, entry.FederatesWith)
	}
	return values
}

// validateRegistrationEntry validates a registration entry for creation
func validateRegistrationEntry(entry *common.RegistrationEntry) error {
	if entry == nil {
		return status.Error(codes.InvalidArgument, "missing registration entry")
	}
	if entry.SpiffeId == "" {
		return status.Error(codes.InvalidArgument, "missing SPIFFE ID")
	}
	if entry.ParentId == "" {
		return status.Error(codes.InvalidArgument, "missing parent ID")
	}
	if len(entry.Selectors) == 0 {
		return status.Error(codes.InvalidArgument, "missing selectors")
	}
	if entry.StoreSvid {
		selectorType := entry.Selectors[0].Type
		for _, s := range entry.Selectors {
			if s.Type != selectorType {
				return status.Error(codes.InvalidArgument, "selector types must be uniform when store_svid is true")
			}
		}
	}
	return nil
}

// validateRegistrationEntryForUpdate validates a registration entry for update
func validateRegistrationEntryForUpdate(entry *common.RegistrationEntry, mask *common.RegistrationEntryMask) error {
	if entry == nil {
		return status.Error(codes.InvalidArgument, "missing registration entry")
	}
	if entry.EntryId == "" {
		return status.Error(codes.InvalidArgument, "missing entry ID")
	}
	if (mask == nil || mask.SpiffeId) && entry.SpiffeId == "" {
		return status.Error(codes.InvalidArgument, "missing SPIFFE ID")
	}
	if (mask == nil || mask.ParentId) && entry.ParentId == "" {
		return status.Error(codes.InvalidArgument, "missing parent ID")
	}
	if (mask == nil || mask.Selectors) && len(entry.Selectors) == 0 {
		return status.Error(codes.InvalidArgument, "missing selectors")
	}
	return nil
}

// getExpiryBucket returns the expiry bucket string for a given expiry timestamp
func getExpiryBucket(expiry int64) string {
	if expiry == 0 {
		return "never"
	}
	t := time.Unix(expiry, 0)
	return t.Format("2006-01")
}

// applyUpdateMask applies the update mask to the existing entry and returns the updated entry
func applyUpdateMask(existing, update *common.RegistrationEntry, mask *common.RegistrationEntryMask) *common.RegistrationEntry {
	result := &common.RegistrationEntry{
		EntryId:        existing.EntryId,
		SpiffeId:       existing.SpiffeId,
		ParentId:       existing.ParentId,
		X509SvidTtl:    existing.X509SvidTtl,
		JwtSvidTtl:     existing.JwtSvidTtl,
		Admin:          existing.Admin,
		Downstream:     existing.Downstream,
		EntryExpiry:    existing.EntryExpiry,
		RevisionNumber: existing.RevisionNumber,
		StoreSvid:      existing.StoreSvid,
		Hint:           existing.Hint,
		Selectors:      existing.Selectors,
		DnsNames:       existing.DnsNames,
		FederatesWith:  existing.FederatesWith,
		CreatedAt:      existing.CreatedAt,
	}

	if mask == nil {
		return update
	}

	if mask.SpiffeId {
		result.SpiffeId = update.SpiffeId
	}
	if mask.ParentId {
		result.ParentId = update.ParentId
	}
	if mask.X509SvidTtl {
		result.X509SvidTtl = update.X509SvidTtl
	}
	if mask.JwtSvidTtl {
		result.JwtSvidTtl = update.JwtSvidTtl
	}
	if mask.Admin {
		result.Admin = update.Admin
	}
	if mask.Downstream {
		result.Downstream = update.Downstream
	}
	if mask.EntryExpiry {
		result.EntryExpiry = update.EntryExpiry
	}
	if mask.StoreSvid {
		result.StoreSvid = update.StoreSvid
	}
	if mask.Hint {
		result.Hint = update.Hint
	}
	if mask.Selectors {
		result.Selectors = update.Selectors
	}
	if mask.DnsNames {
		result.DnsNames = update.DnsNames
	}
	if mask.FederatesWith {
		result.FederatesWith = update.FederatesWith
	}

	return result
}

// listBySpiffeID lists entry IDs by SPIFFE ID with pagination
func listBySpiffeID(ctx context.Context, session *gocql.Session, req *datastore.ListRegistrationEntriesRequest, pageSize int32) ([]string, error) {
	query := `SELECT entry_id FROM registered_entries_by_spiffe WHERE spiffe_id = ?`
	args := []interface{}{req.BySpiffeID}

	if req.Pagination != nil && req.Pagination.Token != "" {
		query += ` AND entry_id > ?`
		args = append(args, req.Pagination.Token)
	}

	query += ` LIMIT ?`
	args = append(args, pageSize+1)

	iter := session.Query(query, args...).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}

	return entryIDs, iter.Close()
}

// listByParentID lists entry IDs by Parent ID with pagination
func listByParentID(ctx context.Context, session *gocql.Session, req *datastore.ListRegistrationEntriesRequest, pageSize int32) ([]string, error) {
	query := `SELECT entry_id FROM registered_entries_by_parent WHERE parent_id = ?`
	args := []interface{}{req.ByParentID}

	if req.Pagination != nil && req.Pagination.Token != "" {
		query += ` AND entry_id > ?`
		args = append(args, req.Pagination.Token)
	}

	query += ` LIMIT ?`
	args = append(args, pageSize+1)

	iter := session.Query(query, args...).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}

	return entryIDs, iter.Close()
}

// listBySelectors lists entry IDs by selectors with matching logic
func listBySelectors(ctx context.Context, session *gocql.Session, selMatch *datastore.BySelectors, pageSize int32) ([]string, error) {
	if len(selMatch.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no selectors provided")
	}

	entryIDSets := make([]map[string]bool, 0, len(selMatch.Selectors))

	for _, selector := range selMatch.Selectors {
		iter := session.Query(`
			SELECT entry_id FROM registered_entries_by_selector 
			WHERE selector_type = ? AND selector_value = ?
		`, selector.Type, selector.Value).
			WithContext(ctx).
			Consistency(gocql.LocalQuorum).
			Iter()

		idSet := make(map[string]bool)
		var entryID string
		for iter.Scan(&entryID) {
			idSet[entryID] = true
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}

		entryIDSets = append(entryIDSets, idSet)
	}

	var resultSet map[string]bool

	switch selMatch.Match {
	case datastore.MatchAny:
		resultSet = make(map[string]bool)
		for _, idSet := range entryIDSets {
			for id := range idSet {
				resultSet[id] = true
			}
		}

	case datastore.Exact, datastore.Subset:
		if len(entryIDSets) == 0 {
			return nil, nil
		}
		resultSet = entryIDSets[0]
		for i := 1; i < len(entryIDSets); i++ {
			resultSet = intersectSets(resultSet, entryIDSets[i])
		}

	case datastore.Superset:
		if len(entryIDSets) == 0 {
			return nil, nil
		}
		resultSet = entryIDSets[0]
		for i := 1; i < len(entryIDSets); i++ {
			resultSet = intersectSets(resultSet, entryIDSets[i])
		}
	}

	entryIDs := make([]string, 0, len(resultSet))
	for id := range resultSet {
		entryIDs = append(entryIDs, id)
	}

	if int32(len(entryIDs)) > pageSize {
		entryIDs = entryIDs[:pageSize]
	}

	return entryIDs, nil
}

// listByHint lists entry IDs by hint with pagination
func listByHint(ctx context.Context, session *gocql.Session, hint string, pageSize int32) ([]string, error) {
	query := `SELECT entry_id FROM registered_entries_by_hint WHERE hint = ? LIMIT ?`
	iter := session.Query(query, hint, pageSize+1).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}

	return entryIDs, iter.Close()
}

// listByFederatesWith lists entry IDs by federated trust domains with matching logic
func listByFederatesWith(ctx context.Context, session *gocql.Session, fedMatch *datastore.ByFederatesWith, pageSize int32) ([]string, error) {
	if len(fedMatch.TrustDomains) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no trust domains provided")
	}

	entryIDSets := make([]map[string]bool, 0, len(fedMatch.TrustDomains))

	for _, td := range fedMatch.TrustDomains {
		iter := session.Query(`
			SELECT entry_id FROM entries_by_federated_td WHERE trust_domain = ?
		`, td).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

		idSet := make(map[string]bool)
		var entryID string
		for iter.Scan(&entryID) {
			idSet[entryID] = true
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}

		entryIDSets = append(entryIDSets, idSet)
	}

	var resultSet map[string]bool

	switch fedMatch.Match {
	case datastore.MatchAny:
		resultSet = make(map[string]bool)
		for _, idSet := range entryIDSets {
			for id := range idSet {
				resultSet[id] = true
			}
		}

	case datastore.Exact, datastore.Subset:
		if len(entryIDSets) == 0 {
			return nil, nil
		}
		resultSet = entryIDSets[0]
		for i := 1; i < len(entryIDSets); i++ {
			resultSet = intersectSets(resultSet, entryIDSets[i])
		}

	case datastore.Superset:
		if len(entryIDSets) == 0 {
			return nil, nil
		}
		resultSet = entryIDSets[0]
		for i := 1; i < len(entryIDSets); i++ {
			resultSet = intersectSets(resultSet, entryIDSets[i])
		}
	}

	entryIDs := make([]string, 0, len(resultSet))
	for id := range resultSet {
		entryIDs = append(entryIDs, id)
	}

	if int32(len(entryIDs)) > pageSize {
		entryIDs = entryIDs[:pageSize]
	}

	return entryIDs, nil
}

// listAllEntries lists all entry IDs with pagination
func listAllEntries(ctx context.Context, session *gocql.Session, pagination *datastore.Pagination, pageSize int32) ([]string, error) {
	query := `SELECT entry_id FROM registered_entries`
	var args []interface{}

	if pagination != nil && pagination.Token != "" {
		query += ` WHERE token(entry_id) > token(?)`
		args = append(args, pagination.Token)
	}

	query += ` LIMIT ?`
	args = append(args, pageSize+1)

	iter := session.Query(query, args...).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var entryIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		entryIDs = append(entryIDs, entryID)
	}

	return entryIDs, iter.Close()
}

// applyClientSideFilters applies additional filters that cannot be handled by Cassandra queries
func applyClientSideFilters(entries []*common.RegistrationEntry, req *datastore.ListRegistrationEntriesRequest) []*common.RegistrationEntry {
	if req.ByDownstream != nil {
		filtered := make([]*common.RegistrationEntry, 0, len(entries))
		for _, entry := range entries {
			if entry.Downstream == *req.ByDownstream {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}

	if req.BySelectors != nil && (req.BySelectors.Match == datastore.Exact || req.BySelectors.Match == datastore.Subset) {
		selectorSet := make(map[string]bool)
		for _, s := range req.BySelectors.Selectors {
			selectorSet[fmt.Sprintf("%s:%s", s.Type, s.Value)] = true
		}

		filtered := make([]*common.RegistrationEntry, 0, len(entries))
		for _, entry := range entries {
			allMatch := true
			for _, s := range entry.Selectors {
				key := fmt.Sprintf("%s:%s", s.Type, s.Value)
				if !selectorSet[key] {
					allMatch = false
					break
				}
			}
			if allMatch {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}

	return entries
}

// intersectSets returns the intersection of two sets represented as maps
func intersectSets(set1, set2 map[string]bool) map[string]bool {
	result := make(map[string]bool)
	for key := range set1 {
		if set2[key] {
			result[key] = true
		}
	}
	return result
}

// lookupSimilarEntry looks for an existing registration entry that is similar to the given one
func lookupSimilarEntry(ctx context.Context, session *gocql.Session, entry *common.RegistrationEntry) (*common.RegistrationEntry, error) {
	// Query by parent and selector
	iter := session.Query(`
		SELECT entry_id FROM registered_entries_by_parent 
		WHERE parent_id = ?
	`, entry.ParentId).WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()

	var candidateIDs []string
	var entryID string
	for iter.Scan(&entryID) {
		candidateIDs = append(candidateIDs, entryID)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	// Fetch and compare
	entryMap, err := fetchRegistrationEntries(ctx, session, candidateIDs)
	if err != nil {
		return nil, err
	}

	for _, candidate := range entryMap {
		if isSimilarEntry(entry, candidate) {
			return candidate, nil
		}
	}

	return nil, nil
}

// isSimilarEntry checks if two registration entries are similar based on SPIFFE ID, Parent ID, and Selectors
func isSimilarEntry(a, b *common.RegistrationEntry) bool {
	if a.SpiffeId != b.SpiffeId || a.ParentId != b.ParentId {
		return false
	}

	if len(a.Selectors) != len(b.Selectors) {
		return false
	}

	aSet := make(map[string]bool)
	for _, s := range a.Selectors {
		aSet[fmt.Sprintf("%s:%s", s.Type, s.Value)] = true
	}

	for _, s := range b.Selectors {
		if !aSet[fmt.Sprintf("%s:%s", s.Type, s.Value)] {
			return false
		}
	}

	return true
}

// fetchRegistrationEntryWithWriteTime retrieves a registration entry along with the writetime
// of one of its columns (typically used for optimistic concurrency control / CAS).
func fetchRegistrationEntryWithWriteTime(ctx context.Context, session *gocql.Session, entryID string) (*common.RegistrationEntry, int64, error) {
	var (
		spiffeID              string
		parentID              string
		ttl                   int32
		jwtSvidTTL            int32
		admin                 bool
		downstream            bool
		expiry                int64
		revisionNumber        int64
		storeSvid             bool
		hint                  string
		selectorSet           []string // set<text> containing "type:value"
		dnsNames              []string
		federatedTrustDomains []string
		createdAt             time.Time
		updatedAt             time.Time
		writeTime             int64 // WRITETIME of one column (e.g. spiffe_id)
	)

	query := `
		SELECT 
			spiffe_id, 
			parent_id, 
			ttl, 
			jwt_svid_ttl, 
			admin, 
			downstream, 
			expiry, 
			revision_number, 
			store_svid, 
			hint, 
			selectors, 
			dns_names, 
			federated_trust_domains, 
			created_at, 
			updated_at, 
			WRITETIME(spiffe_id) AS writetime_spiffe_id
		FROM registered_entries 
		WHERE entry_id = ?
	`

	err := session.Query(query, entryID).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(
			&spiffeID,
			&parentID,
			&ttl,
			&jwtSvidTTL,
			&admin,
			&downstream,
			&expiry,
			&revisionNumber,
			&storeSvid,
			&hint,
			&selectorSet,
			&dnsNames,
			&federatedTrustDomains,
			&createdAt,
			&updatedAt,
			&writeTime,
		)

	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("failed to fetch registration entry %s: %w", entryID, err)
	}

	// Convert string representation "type:value" back to Selector structs
	selectors := make([]*common.Selector, 0, len(selectorSet))
	for _, s := range selectorSet {
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			selectors = append(selectors, &common.Selector{
				Type:  parts[0],
				Value: parts[1],
			})
		} else {
			continue
		}
	}

	entry := &common.RegistrationEntry{
		EntryId:        entryID,
		SpiffeId:       spiffeID,
		ParentId:       parentID,
		X509SvidTtl:    ttl,
		JwtSvidTtl:     jwtSvidTTL,
		Admin:          admin,
		Downstream:     downstream,
		EntryExpiry:    expiry,
		RevisionNumber: revisionNumber,
		StoreSvid:      storeSvid,
		Hint:           hint,
		Selectors:      selectors,
		DnsNames:       dnsNames,
		FederatesWith:  federatedTrustDomains,
		CreatedAt:      createdAt.Unix(),
	}

	return entry, writeTime, nil
}
