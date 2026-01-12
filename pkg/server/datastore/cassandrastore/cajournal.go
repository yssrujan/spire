package cassandrastore

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/private/server/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// Schema Definition:
// CREATE TABLE IF NOT EXISTS ca_journals (
//      active_x509_authority_id text,
//      data blob,
//      created_at timestamp,
//      updated_at timestamp,
//      PRIMARY KEY (active_x509_authority_id)
// )

// DataStore defines the interface for managing CA journals
//   SetCAJournal(ctx context.Context, caJournal *CAJournal) (*CAJournal, error)
//   FetchCAJournal(ctx context.Context, activeX509AuthorityID string) (*CAJournal, error)
//   PruneCAJournals(ctx context.Context, allCAsExpireBefore int64) error
//   ListCAJournalsForTesting(ctx context.Context) ([]*CAJournal, error)

// CAJournal represents a CA journal entry
type CAJournal struct {
	ID        uint64
	CreatedAt time.Time
	UpdatedAt time.Time

	Data                  []byte
	ActiveX509AuthorityID string
}

// setCAJournal creates or updates a CA journal entry
func setCAJournal(ctx context.Context, session *gocql.Session, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	if err := validateCAJournal(caJournal); err != nil {
		return nil, err
	}

	now := time.Now()

	// First, check if this is a new entry or an update
	_, err := fetchCAJournal(ctx, session, caJournal.ActiveX509AuthorityID)
	if err != nil {
		return nil, err
	}

	// Upsert with all fields
	query := `INSERT INTO ca_journals (
        active_x509_authority_id, 
        data, 
        created_at,
        updated_at
    ) VALUES (?, ?, ?, ?)`

	if err := session.Query(query,
		caJournal.ActiveX509AuthorityID,
		caJournal.Data,
		now,
		now,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("failed to upsert CA journal: %w", err)
	}

	result := &datastore.CAJournal{
		Data:                  caJournal.Data,
		ActiveX509AuthorityID: caJournal.ActiveX509AuthorityID,
	}

	return result, nil
}

// fetchCAJournal retrieves the CA journal for the specified active X509 authority ID
func fetchCAJournal(ctx context.Context, session *gocql.Session, activeX509AuthorityID string) (*datastore.CAJournal, error) {
	if activeX509AuthorityID == "" {
		return nil, status.Error(codes.InvalidArgument, "active X509 authority ID is required")
	}

	query := `SELECT data, created_at, updated_at 
              FROM ca_journals 
              WHERE active_x509_authority_id = ?`

	var data []byte
	var createdAt, updatedAt time.Time

	if err := session.Query(query, activeX509AuthorityID).WithContext(ctx).Scan(
		&data, &createdAt, &updatedAt,
	); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch CA journal: %w", err)
	}

	return &datastore.CAJournal{
		Data:                  data,
		ActiveX509AuthorityID: activeX509AuthorityID,
	}, nil
}

// pruneCAJournals deletes CA journals where all CA entries are expired before the specified timestamp
func pruneCAJournals(ctx context.Context, session *gocql.Session, allCAsExpireBefore int64) error {
	var pagingState []byte
	var totalDeleted int
	deleteBatch := make([]string, 0, maxBatchDeleteSize)

	// Iterate with pagination
	for {
		iter := session.Query(`SELECT active_x509_authority_id, data FROM ca_journals`).
			PageSize(defaultPageSize).
			PageState(pagingState).
			WithContext(ctx).
			Iter()

		var authorityID string
		var data []byte

		for iter.Scan(&authorityID, &data) {
			// Check if this journal should be deleted
			if shouldDeleteJournal(data, allCAsExpireBefore) {
				deleteBatch = append(deleteBatch, authorityID)

				// Process batch when it reaches max size
				if len(deleteBatch) >= maxBatchDeleteSize {
					if err := batchDeleteJournals(ctx, session, deleteBatch); err != nil {
						// Log error but continue - don't fail entire operation
						// In production, use proper logging here
						fmt.Printf("Warning: failed to delete batch: %v\n", err)
					} else {
						totalDeleted += len(deleteBatch)
					}
					deleteBatch = deleteBatch[:0]

					// Small delay to reduce tombstone pressure
					time.Sleep(100 * time.Millisecond)
				}
			}
		}

		// Capture paging state for next iteration
		pagingState = iter.PageState()

		if err := iter.Close(); err != nil {
			return fmt.Errorf("failed to scan CA journals during pruning: %w", err)
		}

		// If no more pages, exit loop
		if len(pagingState) == 0 {
			break
		}

		// Small delay between pages to reduce cluster load
		time.Sleep(50 * time.Millisecond)
	}

	// Delete any remaining journals in the batch
	if len(deleteBatch) > 0 {
		if err := batchDeleteJournals(ctx, session, deleteBatch); err != nil {
			return fmt.Errorf("failed to delete final batch: %w", err)
		}
		totalDeleted += len(deleteBatch)
	}

	return nil
}

// batchDeleteJournals deletes CA journals for the given active X509 authority IDs in a batch
func batchDeleteJournals(ctx context.Context, session *gocql.Session, authorityIDs []string) error {
	if len(authorityIDs) == 0 {
		return nil
	}

	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for _, authID := range authorityIDs {
		batch.Query(`DELETE FROM ca_journals WHERE active_x509_authority_id = ?`, authID)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("failed to batch delete %d CA journals: %w", len(authorityIDs), err)
	}

	return nil
}

// listCAJournalsForTesting lists all CA journals (for testing purposes only)
func listCAJournalsForTesting(ctx context.Context, session *gocql.Session) ([]*datastore.CAJournal, error) {
	iter := session.Query(
		`SELECT active_x509_authority_id, data, created_at, updated_at FROM ca_journals`,
	).WithContext(ctx).Iter()

	var journals []*datastore.CAJournal
	var authorityID string
	var data []byte
	var createdAt, updatedAt time.Time

	for iter.Scan(&authorityID, &data, &createdAt, &updatedAt) {
		journals = append(journals, &datastore.CAJournal{
			Data:                  data,
			ActiveX509AuthorityID: authorityID,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list CA journals: %w", err)
	}

	return journals, nil
}

// deleteJournalEntry deletes a specific CA journal entry
func deleteJournalEntry(ctx context.Context, session *gocql.Session, authID string) error {
	return session.Query(
		`DELETE FROM ca_journals WHERE active_x509_authority_id = ?`,
		authID,
	).WithContext(ctx).Exec()
}

// shouldDeleteJournal checks if all CA entries in the journal are expired
func shouldDeleteJournal(data []byte, limit int64) bool {
	entries := new(journal.Entries)
	if err := proto.Unmarshal(data, entries); err != nil {
		// If we can't unmarshal, keep the journal to be safe
		return false
	}

	// If there are no entries, don't delete (might be a data issue)
	if len(entries.X509CAs) == 0 && len(entries.JwtKeys) == 0 {
		return false
	}

	// If ANY CA is still valid (NotAfter > limit), keep the journal
	for _, x509CA := range entries.X509CAs {
		if x509CA.NotAfter > limit {
			return false
		}
	}

	// If ANY JWT key is still valid, keep the journal
	for _, jwtKey := range entries.JwtKeys {
		if jwtKey.NotAfter > limit {
			return false
		}
	}

	// All entries are expired
	return true
}

// validateCAJournal validates the required fields of a CA journal
func validateCAJournal(caJournal *datastore.CAJournal) error {
	if caJournal == nil {
		return status.Error(codes.InvalidArgument, "ca journal is required")
	}
	if caJournal.ActiveX509AuthorityID == "" {
		return status.Error(codes.InvalidArgument, "active X509 authority ID is required")
	}
	if len(caJournal.Data) == 0 {
		return status.Error(codes.InvalidArgument, "ca journal data is required")
	}
	return nil
}
