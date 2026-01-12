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

// Join Tokens Table Schema:
// `CREATE TABLE IF NOT EXISTS join_tokens (
// 			token_value text,
// 			expiry bigint,
// 			created_at timestamp,
// 			PRIMARY KEY (token_value)
// 		)`,

//  DataStore defines the interface for managing join tokens
//  	CreateJoinToken(context.Context, *JoinToken) error
// 		DeleteJoinToken(ctx context.Context, token string) error
// 		FetchJoinToken(ctx context.Context, token string) (*JoinToken, error)
// 		PruneJoinTokens(context.Context, time.Time) error

// JoinToken represents a join token
type JoinToken struct {
	CreatedAt time.Time

	TokenValue string
	Expiry     int64
}

// createJoinToken creates a new join token with TTL based on expiry
func createJoinToken(ctx context.Context, session *gocql.Session, token *datastore.JoinToken) error {
	if token == nil || token.Token == "" {
		return status.Error(codes.InvalidArgument, "token is required")
	}
	if token.Expiry.IsZero() || token.Expiry.Before(time.Now()) {
		return status.Error(codes.InvalidArgument, "valid future expiry is required")
	}

	ttlSeconds := int64(time.Until(token.Expiry).Seconds())
	if ttlSeconds <= 0 {
		return status.Error(codes.InvalidArgument, "expiry must be in the future")
	}

	now := time.Now()

	// NOTE:
	// Cassandra does NOT support USING TTL with IF NOT EXISTS.
	// SPIRE upstream relies on randomness + TTL, not LWT.
	if err := session.Query(`
		INSERT INTO join_tokens (token_value, expiry, created_at)
		VALUES (?, ?, ?)
		USING TTL ?`,
		token.Token,
		token.Expiry,
		now,
		ttlSeconds,
	).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec(); err != nil {

		return fmt.Errorf("failed to create join token: %w", err)
	}

	return nil
}

// fetchJoinToken retrieves a join token if it exists and has not expired
func fetchJoinToken(ctx context.Context, session *gocql.Session, token string) (*datastore.JoinToken, error) {
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "missing token")
	}

	var expiry time.Time
	var createdAt time.Time

	err := session.Query(`
		SELECT expiry, created_at FROM join_tokens WHERE token_value = ?`,
		token,
	).WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&expiry, &createdAt)

	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch join token: %w", err)
	}

	// Double-check expiry in case TTL hasn't kicked in yet
	if time.Now().After(expiry) {
		return nil, nil
	}

	return &datastore.JoinToken{
		Token:  token,
		Expiry: expiry,
	}, nil
}

// deleteJoinToken deletes a join token explicitly
func deleteJoinToken(ctx context.Context, session *gocql.Session, token string) error {
	if token == "" {
		return status.Error(codes.InvalidArgument, "missing token")
	}

	return session.Query(`
		DELETE FROM join_tokens WHERE token_value = ?`,
		token,
	).WithContext(ctx).
		Consistency(gocql.Quorum).
		Exec()
}

// pruneJoinTokens removes explicitly deleted or long-expired tokens (rare cleanup)
// Note: In normal operation, TTL handles expiration automatically — this is only for cleanup.
func pruneJoinTokens(ctx context.Context, session *gocql.Session, expiresBefore time.Time) error {
	// TTL should handle expiration, but in case of manual deletions or very old tokens, we can prune them here.
	iter := session.Query(`SELECT token_value FROM join_tokens`).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		Iter()

	var token string
	batch := session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.Quorum)

	count := 0
	for iter.Scan(&token) {
		batch.Query(`DELETE FROM join_tokens WHERE token_value = ?`, token)
		count++
		if count >= 100 {
			if err := session.ExecuteBatch(batch); err != nil {
				return fmt.Errorf("failed to prune batch: %w", err)
			}
			batch = session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
			batch.SetConsistency(gocql.Quorum)
			count = 0
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to scan tokens: %w", err)
	}
	if count > 0 {
		if err := session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("failed to prune final batch: %w", err)
		}
	}

	return nil
}
