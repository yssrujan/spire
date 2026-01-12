package cassandrastore

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/spiffe/spire/pkg/server/datastore"
	"google.golang.org/grpc/codes"
)

// Attested Node Events Table Schema:
// CREATE TABLE IF NOT EXISTS attested_node_events (
//     spiffe_id text,
//     event_id timeuuid,            -- Time-based UUID for ordering
//     created_at timestamp,
//     PRIMARY KEY (spiffe_id, event_id)
// ) WITH CLUSTERING ORDER BY (event_id DESC)

// DataStore defines the interface for managing attested node events
// 		ListAttestedNodeEvents(ctx context.Context, req *ListAttestedNodeEventsRequest) (*ListAttestedNodeEventsResponse, error)
// 		PruneAttestedNodeEvents(ctx context.Context, olderThan time.Duration) error
// 		FetchAttestedNodeEvent(ctx context.Context, eventID uint) (*AttestedNodeEvent, error)
// 		CreateAttestedNodeEventForTesting(ctx context.Context, event *AttestedNodeEvent) error
// 		DeleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error

// AttestedNodeEvent represents an attested node event as attested_node_events table
type AttestedNodeEvent struct {
	ID        gocql.UUID
	CreatedAt time.Time
	UpdatedAt time.Time

	EventID  gocql.UUID
	SpiffeID string
}

func createAttestedNodeEvent(ctx context.Context, session *gocql.Session, spiffeID string) error {
	eventID := gocql.TimeUUID()
	now := time.Now()
	bucket := int(eventID.Time().Unix() % eventBucketCount)

	if err := session.Query(
		`INSERT INTO attested_node_events (event_id, spiffe_id, created_at) VALUES (?, ?, ?)`,
		eventID, spiffeID, now,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to create node event: %w", err)
	}

	if err := session.Query(
		`INSERT INTO attested_node_events_by_time (bucket, event_id, spiffe_id) VALUES (?, ?, ?)`,
		bucket, eventID, spiffeID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to create time-indexed node event: %w", err)
	}

	return nil
}

func listAttestedNodeEvents(ctx context.Context, session *gocql.Session, req *datastore.ListAttestedNodeEventsRequest) (*datastore.ListAttestedNodeEventsResponse, error) {
	var events []datastore.AttestedNodeEvent

	for bucket := 0; bucket < eventBucketCount; bucket++ {
		iter := session.Query(
			`SELECT event_id, spiffe_id FROM attested_node_events_by_time WHERE bucket = ?`,
			bucket,
		).WithContext(ctx).Iter()

		var eventID gocql.UUID
		var spiffeID string
		for iter.Scan(&eventID, &spiffeID) {
			events = append(events, datastore.AttestedNodeEvent{
				EventID:  uint(eventID.Time().Unix()),
				SpiffeID: spiffeID,
			})
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to list node events: %w", err)
		}
	}

	return &datastore.ListAttestedNodeEventsResponse{
		Events: events,
	}, nil
}

func fetchAttestedNodeEvent(ctx context.Context, session *gocql.Session, eventID uint) (*datastore.AttestedNodeEvent, error) {
	// Convert uint to approximate timestamp for query
	eventTime := time.Unix(int64(eventID), 0)

	iter := session.Query(
		`SELECT event_id, spiffe_id FROM attested_node_events WHERE created_at >= ? AND created_at <= ? ALLOW FILTERING`,
		eventTime.Add(-time.Hour), eventTime.Add(time.Hour),
	).WithContext(ctx).Iter()

	var foundEventID gocql.UUID
	var spiffeID string
	for iter.Scan(&foundEventID, &spiffeID) {
		if uint(foundEventID.Time().Unix()) == eventID {
			if err := iter.Close(); err != nil {
				return nil, fmt.Errorf("failed to fetch event: %w", err)
			}
			return &datastore.AttestedNodeEvent{
				EventID:  eventID,
				SpiffeID: spiffeID,
			}, nil
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch event: %w", err)
	}

	return nil, status.Error(codes.NotFound, "event not found")
}

func pruneAttestedNodeEvents(ctx context.Context, session *gocql.Session, olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan)

	for bucket := 0; bucket < eventBucketCount; bucket++ {
		iter := session.Query(
			`SELECT event_id FROM attested_node_events_by_time WHERE bucket = ?`,
			bucket,
		).WithContext(ctx).Iter()

		var eventID gocql.UUID
		toDelete := []gocql.UUID{}
		for iter.Scan(&eventID) {
			if eventID.Time().Before(cutoff) {
				toDelete = append(toDelete, eventID)
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("failed to list node events: %w", err)
		}

		for _, id := range toDelete {
			if err := session.Query(
				`DELETE FROM attested_node_events WHERE event_id = ?`,
				id,
			).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("failed to delete node event: %w", err)
			}

			if err := session.Query(
				`DELETE FROM attested_node_events_by_time WHERE bucket = ? AND event_id = ?`,
				bucket, id,
			).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("failed to delete time-indexed node event: %w", err)
			}
		}
	}

	return nil
}

func deleteAttestedNodeEvent(ctx context.Context, session *gocql.Session, eventID uint) error {
	return nil
}

func createAttestedNodeEventForTesting(ctx context.Context, session *gocql.Session, event *datastore.AttestedNodeEvent) error {
	return createAttestedNodeEvent(ctx, session, event.SpiffeID)
}

func deleteAttestedNodeEventForTesting(ctx context.Context, session *gocql.Session, eventID uint) error {
	// Convert uint to approximate timestamp
	eventTime := time.Unix(int64(eventID), 0)

	// Find the exact event
	iter := session.Query(
		`SELECT event_id FROM attested_node_events WHERE created_at >= ? AND created_at <= ? ALLOW FILTERING`,
		eventTime.Add(-time.Hour), eventTime.Add(time.Hour),
	).WithContext(ctx).Iter()

	var foundEventID gocql.UUID
	var targetEventID *gocql.UUID
	for iter.Scan(&foundEventID) {
		if uint(foundEventID.Time().Unix()) == eventID {
			tmp := foundEventID
			targetEventID = &tmp
			break
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to find event: %w", err)
	}

	if targetEventID == nil {
		return status.Error(codes.NotFound, "event not found")
	}

	// Delete from main table
	if err := session.Query(
		`DELETE FROM attested_node_events WHERE event_id = ?`,
		*targetEventID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to delete event: %w", err)
	}

	// Delete from time-bucketed table
	bucket := int(targetEventID.Time().Unix() % eventBucketCount)
	if err := session.Query(
		`DELETE FROM attested_node_events_by_time WHERE bucket = ? AND event_id = ?`,
		bucket, *targetEventID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to delete time-indexed event: %w", err)
	}

	return nil
}
