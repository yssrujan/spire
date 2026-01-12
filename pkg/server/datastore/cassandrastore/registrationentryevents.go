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

// Registration Entry Events Table Schema:
// CREATE TABLE IF NOT EXISTS registered_entry_events (
// 	   partition int
// 	   entry_id text,
//     event_id timeuuid,
//     PRIMARY KEY (partition, event_id)
// ) WITH CLUSTERING ORDER BY (event_id DESC)

// DataStore defines the interface for managing registration entry events
// ListAttestedNodeEvents(ctx context.Context, req *ListAttestedNodeEventsRequest) (*ListAttestedNodeEventsResponse, error)
// PruneAttestedNodeEvents(ctx context.Context, olderThan time.Duration) error
// FetchAttestedNodeEvent(ctx context.Context, eventID uint) (*AttestedNodeEvent, error)
// CreateAttestedNodeEventForTesting(ctx context.Context, event *AttestedNodeEvent) error
// DeleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error

// RegisteredEntryEvent represents a registration entry event as registered_entry_events table
type RegisteredEntryEvent struct {
	ID        gocql.UUID
	CreatedAt time.Time
	UpdatedAt time.Time

	EventID gocql.UUID
	EntryID string
}

const (
	registrationEntryEventsPartition = 0
	defaultEventPageSize             = 100
)

// createRegistrationEntryEvent creates a new registration entry event
func createRegistrationEntryEvent(
	ctx context.Context,
	session *gocql.Session,
	entryID string,
) error {

	if entryID == "" {
		return status.Error(codes.InvalidArgument, "missing entry ID")
	}

	eventUUID := gocql.TimeUUID()

	if err := session.Query(
		`INSERT INTO registered_entry_events (partition, event_id, entry_id)
		 VALUES (?, ?, ?)`,
		registrationEntryEventsPartition,
		eventUUID,
		entryID,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec(); err != nil {
		return fmt.Errorf("failed to create registration entry event: %w", err)
	}

	return nil
}

// listRegistrationEntryEvents lists registration entry events with pagination
func listRegistrationEntryEvents(
	ctx context.Context,
	session *gocql.Session,
	req *datastore.ListRegistrationEntryEventsRequest,
) (*datastore.ListRegistrationEntryEventsResponse, error) {

	query := `
		SELECT event_id, entry_id
		FROM registered_entry_events
		WHERE partition = ?
	`
	args := []interface{}{registrationEntryEventsPartition}

	// DESC order:
	// - LessThanEventID → older events
	// - GreaterThanEventID → newer events
	if req.LessThanEventID != 0 {
		query += " AND event_id < ?"
		args = append(args, gocql.MinTimeUUID(time.Unix(0, int64(req.LessThanEventID))))
	}
	if req.GreaterThanEventID != 0 {
		query += " AND event_id > ?"
		args = append(args, gocql.MaxTimeUUID(time.Unix(0, int64(req.GreaterThanEventID))))
	}

	iter := session.Query(query, args...).
		PageSize(defaultEventPageSize + 1).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Iter()

	var (
		uuid    gocql.UUID
		entryID string
		events  []datastore.RegistrationEntryEvent
	)

	for iter.Scan(&uuid, &entryID) {
		events = append(events, datastore.RegistrationEntryEvent{
			EventID: uint(uuid.Time().UnixNano()),
			EntryID: entryID,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list registration entry events: %w", err)
	}

	resp := &datastore.ListRegistrationEntryEventsResponse{}

	if len(events) > defaultEventPageSize {
		resp.Events = events[:defaultEventPageSize]
		// Next page cursor = first element of next page
		resp.Events = events[:defaultEventPageSize]
	} else {
		resp.Events = events
	}

	return resp, nil
}

// fetchRegistrationEntryEvent fetches a specific registration entry event by its event ID
func fetchRegistrationEntryEvent(
	ctx context.Context,
	session *gocql.Session,
	eventID uint,
) (*datastore.RegistrationEntryEvent, error) {

	eventUUID := gocql.MinTimeUUID(time.Unix(0, int64(eventID)))

	var entryID string

	if err := session.Query(
		`SELECT entry_id
		 FROM registered_entry_events
		 WHERE partition = ? AND event_id = ?`,
		registrationEntryEventsPartition,
		eventUUID,
	).
		WithContext(ctx).
		Consistency(gocql.One).
		Scan(&entryID); err != nil {

		if err == gocql.ErrNotFound {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		return nil, fmt.Errorf("failed to fetch registration entry event: %w", err)
	}

	return &datastore.RegistrationEntryEvent{
		EventID: eventID,
		EntryID: entryID,
	}, nil
}

// pruneRegistrationEntryEvents prunes registration entry events older than the specified duration
func pruneRegistrationEntryEvents(
	ctx context.Context,
	session *gocql.Session,
	olderThan time.Duration,
) error {

	cutoff := gocql.MinTimeUUID(time.Now().Add(-olderThan))

	if err := session.Query(
		`DELETE FROM registered_entry_events
		 WHERE partition = ? AND event_id < ?`,
		registrationEntryEventsPartition,
		cutoff,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec(); err != nil {
		return fmt.Errorf("failed to prune registration entry events: %w", err)
	}

	return nil
}

// createRegistrationEntryEventForTesting creates a registration entry event for testing purposes
func createRegistrationEntryEventForTesting(
	ctx context.Context,
	session *gocql.Session,
	event *datastore.RegistrationEntryEvent,
) error {
	if event == nil {
		return status.Error(codes.InvalidArgument, "nil event")
	}
	return createRegistrationEntryEvent(ctx, session, event.EntryID)
}

// deleteRegistrationEntryEventForTesting deletes a registration entry event for testing purposes
func deleteRegistrationEntryEventForTesting(
	ctx context.Context,
	session *gocql.Session,
	eventID uint,
) error {

	eventUUID := gocql.MinTimeUUID(time.Unix(0, int64(eventID)))

	if err := session.Query(
		`DELETE FROM registered_entry_events
		 WHERE partition = ? AND event_id = ?`,
		registrationEntryEventsPartition,
		eventUUID,
	).
		WithContext(ctx).
		Exec(); err != nil {
		return fmt.Errorf("failed to delete registration entry event: %w", err)
	}

	return nil
}
