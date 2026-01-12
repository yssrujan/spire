package cassandrastore

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"github.com/spiffe/spire/pkg/server/datastore"
	"google.golang.org/grpc/codes"
)

// Schema Definition:
// CREATE TABLE IF NOT EXISTS federated_trust_domains (
//     trust_domain text PRIMARY KEY,
//     bundle_endpoint_url text,
//     bundle_endpoint_profile text,  -- 'https_web' or 'https_spiffe'
//     endpoint_spiffe_id text,       -- Required if profile = 'https_spiffe'
//     implicit boolean,              -- Auto-federate with all entries
//     version bigint,
//     created_at timestamp,
//     updated_at timestamp
// )

// CREATE TABLE IF NOT EXISTS entries_federated_with (
//     entry_id text,
//     trust_domain text,
//     created_at timestamp,
//     PRIMARY KEY (entry_id, trust_domain)
// ) WITH CLUSTERING ORDER BY (trust_domain ASC)

// CREATE TABLE IF NOT EXISTS entries_by_federated_td (
//     trust_domain text,
//     entry_id text,
//     spiffe_id text,
//     PRIMARY KEY (trust_domain, entry_id)
// ) WITH CLUSTERING ORDER BY (entry_id ASC)

// DataStore defines the interface for managing federation relationships
//
//	CreateFederationRelationship(ctx context.Context, fr *FederationRelationship) (*FederationRelationship, error)
//	FetchFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) (*FederationRelationship, error)
//	UpdateFederationRelationship(ctx context.Context, fr *FederationRelationship, mask *types.FederationRelationshipMask) (*FederationRelationship, error)
//	DeleteFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) error
//	ListFederationRelationships(ctx context.Context, req *ListFederationRelationshipsRequest) (*ListFederationRelationshipsResponse, error)

// FederatedTrustDomain represents a federation relationship
type FederatedTrustDomain struct {
	TrustDomain           string
	BundleEndpointURL     string
	BundleEndpointProfile string
	EndpointSPIFFEID      string
	Implicit              bool
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// createFederationRelationship creates a new federation relationship
func createFederationRelationship(ctx context.Context, session *gocql.Session, fr *datastore.FederationRelationship) (*datastore.FederationRelationship, error) {
	if err := validateFederationRelationship(fr, nil); err != nil {
		return nil, err
	}

	now := time.Now()
	initialVersion := int64(1)

	// Normalize URL to prevent unnecessary updates
	normalizedURL := normalizeURL(fr.BundleEndpointURL)

	endpointSPIFFEID := ""
	if !fr.EndpointSPIFFEID.IsZero() {
		endpointSPIFFEID = fr.EndpointSPIFFEID.String()
	}

	// This prevents orphan federation rows if bundle write fails
	if fr.TrustDomainBundle != nil {
		if _, err := setBundle(ctx, session, fr.TrustDomainBundle); err != nil {
			return nil, fmt.Errorf("failed to store bundle: %w", err)
		}
	}

	query := `INSERT INTO federated_trust_domains (
        trust_domain, bundle_endpoint_url, bundle_endpoint_profile, endpoint_spiffe_id,
        implicit, version, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	applied, err := session.Query(query,
		fr.TrustDomain.Name(),
		normalizedURL.String(),
		string(fr.BundleEndpointProfile),
		endpointSPIFFEID,
		false, // implicit - default to false
		initialVersion,
		now,
		now,
	).WithContext(ctx).
		Consistency(gocql.Quorum).
		MapScanCAS(map[string]interface{}{})

	if err != nil {
		return nil, fmt.Errorf("failed to create federation relationship: %w", err)
	}

	if !applied {
		return nil, status.Errorf(codes.AlreadyExists,
			"federation relationship for trust domain %q already exists", fr.TrustDomain.Name())
	}

	// Return complete object with timestamps and version
	result := &datastore.FederationRelationship{
		TrustDomain:           fr.TrustDomain,
		BundleEndpointURL:     normalizedURL,
		BundleEndpointProfile: fr.BundleEndpointProfile,
		EndpointSPIFFEID:      fr.EndpointSPIFFEID,
		TrustDomainBundle:     fr.TrustDomainBundle,
	}

	return result, nil
}

// fetchFederationRelationship retrieves a federation relationship by trust domain
func fetchFederationRelationship(ctx context.Context, session *gocql.Session, trustDomain spiffeid.TrustDomain) (*datastore.FederationRelationship, error) {
	if trustDomain.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "trust domain is required")
	}

	var (
		bundleEndpointURLStr  string
		bundleEndpointProfile string
		endpointSPIFFEID      string
		implicit              bool
		version               int64
		createdAt, updatedAt  time.Time
	)

	query := `SELECT bundle_endpoint_url, bundle_endpoint_profile, endpoint_spiffe_id, 
                     implicit, version, created_at, updated_at
              FROM federated_trust_domains 
              WHERE trust_domain = ?`

	if err := session.Query(query, trustDomain.Name()).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&bundleEndpointURLStr, &bundleEndpointProfile, &endpointSPIFFEID,
			&implicit, &version, &createdAt, &updatedAt); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch federation relationship: %w", err)
	}

	return constructFederationRelationship(ctx, session, trustDomain, bundleEndpointURLStr,
		bundleEndpointProfile, endpointSPIFFEID, implicit, version, createdAt, updatedAt)
}

// updateFederationRelationship updates an existing federation relationship with optimistic locking
func updateFederationRelationship(ctx context.Context, session *gocql.Session, fr *datastore.FederationRelationship, mask *types.FederationRelationshipMask) (*datastore.FederationRelationship, error) {
	if err := validateFederationRelationship(fr, mask); err != nil {
		return nil, err
	}

	var currentVersion int64
	query := `SELECT version FROM federated_trust_domains WHERE trust_domain = ?`

	if err := session.Query(query, fr.TrustDomain.Name()).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Scan(&currentVersion); err != nil {
		if err == gocql.ErrNotFound {
			return nil, status.Errorf(codes.NotFound,
				"federation relationship for trust domain %q not found", fr.TrustDomain.Name())
		}
		return nil, fmt.Errorf("failed to fetch current version: %w", err)
	}

	// Build dynamic UPDATE query with CAS
	updates := []string{}
	values := []interface{}{}

	now := time.Now()
	newVersion := currentVersion + 1

	updates = append(updates, "updated_at = ?", "version = ?")
	values = append(values, now, newVersion)

	if mask.BundleEndpointUrl {
		normalizedURL := normalizeURL(fr.BundleEndpointURL)
		updates = append(updates, "bundle_endpoint_url = ?")
		values = append(values, normalizedURL.String())
	}

	if mask.BundleEndpointProfile {
		updates = append(updates, "bundle_endpoint_profile = ?")
		values = append(values, string(fr.BundleEndpointProfile))

		// Data consistency: manage endpoint_spiffe_id based on profile
		if fr.BundleEndpointProfile == datastore.BundleEndpointSPIFFE {
			if !fr.EndpointSPIFFEID.IsZero() {
				updates = append(updates, "endpoint_spiffe_id = ?")
				values = append(values, fr.EndpointSPIFFEID.String())
			}
		} else {
			// Clear SPIFFE ID when switching to non-SPIFFE profile
			updates = append(updates, "endpoint_spiffe_id = ?")
			values = append(values, "")
		}
	}

	// Add WHERE and IF conditions
	values = append(values, fr.TrustDomain.Name(), currentVersion)

	query = fmt.Sprintf(`UPDATE federated_trust_domains SET %s 
		WHERE trust_domain = ? IF version = ?`,
		strings.Join(updates, ", "))

	// Execute CAS update
	applied, err := session.Query(query, values...).
		WithContext(ctx).
		Consistency(gocql.Quorum).
		MapScanCAS(map[string]interface{}{})

	if err != nil {
		return nil, fmt.Errorf("failed to update federation relationship: %w", err)
	}

	if !applied {
		return nil, status.Error(codes.Aborted,
			"federation relationship was modified concurrently, please retry")
	}

	// Update bundle if specified
	if mask.TrustDomainBundle && fr.TrustDomainBundle != nil {
		if _, err := setBundle(ctx, session, fr.TrustDomainBundle); err != nil {
			return nil, fmt.Errorf("failed to update bundle: %w", err)
		}
	}

	return fetchFederationRelationship(ctx, session, fr.TrustDomain)
}

// deleteFederationRelationship deletes a federation relationship and ALL associated data
func deleteFederationRelationship(ctx context.Context, session *gocql.Session, trustDomain spiffeid.TrustDomain) error {
	if trustDomain.IsZero() {
		return status.Error(codes.InvalidArgument, "trust domain is required")
	}

	iter := session.Query(
		`SELECT entry_id FROM entries_by_federated_td WHERE trust_domain = ?`,
		trustDomain.Name(),
	).WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Iter()

	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	var entryID string
	entryCount := 0
	for iter.Scan(&entryID) {
		// Delete from entries_federated_with (reverse index)
		batch.Query(
			`DELETE FROM entries_federated_with WHERE entry_id = ? AND trust_domain = ?`,
			entryID, trustDomain.Name(),
		)
		entryCount++

		// Process in smaller batches to avoid coordinator overload
		if entryCount >= 50 {
			if err := session.ExecuteBatch(batch); err != nil {
				iter.Close()
				return fmt.Errorf("failed to delete federation entry associations: %w", err)
			}
			batch = session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
			entryCount = 0
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("failed to scan federated entries: %w", err)
	}

	// Delete remaining batch items plus the primary tables
	batch.Query(
		`DELETE FROM entries_by_federated_td WHERE trust_domain = ?`,
		trustDomain.Name(),
	)
	batch.Query(
		`DELETE FROM federated_trust_domains WHERE trust_domain = ?`,
		trustDomain.Name(),
	)

	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("failed to delete federation relationship: %w", err)
	}

	return nil
}

// listFederationRelationships lists federation relationships with safe pagination
func listFederationRelationships(ctx context.Context, session *gocql.Session, req *datastore.ListFederationRelationshipsRequest, log logrus.FieldLogger) (*datastore.ListFederationRelationshipsResponse, error) {
	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	pageSize := defaultPageSize
	if req.Pagination != nil && req.Pagination.PageSize > 0 {
		pageSize = int(req.Pagination.PageSize)
	}

	var pagingState []byte
	if req.Pagination != nil && req.Pagination.Token != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.Pagination.Token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
		}
		pagingState = decoded
	}

	query := `SELECT trust_domain, bundle_endpoint_url, bundle_endpoint_profile, 
                     endpoint_spiffe_id, implicit, version, created_at, updated_at
              FROM federated_trust_domains`

	iter := session.Query(query).
		PageSize(pageSize + 1).
		PageState(pagingState).
		WithContext(ctx).
		Consistency(gocql.LocalQuorum).
		Iter()

	var (
		trustDomainStr        string
		bundleEndpointURLStr  string
		bundleEndpointProfile string
		endpointSPIFFEID      string
		implicit              bool
		version               int64
		createdAt, updatedAt  time.Time
	)

	relationships := []*datastore.FederationRelationship{}
	skippedCount := 0

	for iter.Scan(&trustDomainStr, &bundleEndpointURLStr, &bundleEndpointProfile,
		&endpointSPIFFEID, &implicit, &version, &createdAt, &updatedAt) {

		td, err := spiffeid.TrustDomainFromString(trustDomainStr)
		if err != nil {
			log.WithError(err).WithField("trust_domain", trustDomainStr).Warn("Skipping invalid trust domain in federation relationship")
			skippedCount++
			continue
		}

		fr, err := constructFederationRelationship(ctx, session, td, bundleEndpointURLStr,
			bundleEndpointProfile, endpointSPIFFEID, implicit, version, createdAt, updatedAt)
		if err != nil {
			log.WithError(err).WithField("trust_domain", td.String()).Warn("Skipping invalid federation relationship")
			skippedCount++
			continue
		}

		relationships = append(relationships, fr)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list federation relationships: %w", err)
	}

	if skippedCount > 0 {
		log.WithField("count", skippedCount).Warn("Some federation relationships were skipped due to errors")
	}

	resp := &datastore.ListFederationRelationshipsResponse{
		FederationRelationships: relationships,
	}

	if req.Pagination != nil {
		resp.Pagination = &datastore.Pagination{
			PageSize: int32(pageSize),
		}

		if len(relationships) > pageSize {
			resp.FederationRelationships = relationships[:pageSize]

			nextPageState := iter.PageState()
			if len(nextPageState) > 0 {
				resp.Pagination.Token = base64.StdEncoding.EncodeToString(nextPageState)
			}
		}
	}

	return resp, nil
}

// constructFederationRelationship builds a complete FederationRelationship object
func constructFederationRelationship(ctx context.Context, session *gocql.Session,
	td spiffeid.TrustDomain, urlStr, profile, spiffeID string, implicit bool,
	version int64, createdAt, updatedAt time.Time) (*datastore.FederationRelationship, error) {

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid bundle endpoint URL in database: %w", err)
	}

	fr := &datastore.FederationRelationship{
		TrustDomain:           td,
		BundleEndpointURL:     u,
		BundleEndpointProfile: datastore.BundleEndpointType(profile),
	}

	if spiffeID != "" {
		id, err := spiffeid.FromString(spiffeID)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint SPIFFE ID in database: %w", err)
		}
		fr.EndpointSPIFFEID = id
	}

	// Fetch associated bundle (don't fail if missing - might not be synced yet)
	bundle, err := fetchBundle(ctx, session, td.IDString())
	if err == nil {
		fr.TrustDomainBundle = bundle
	}

	return fr, nil
}

// normalizeURL ensures consistent URL formatting to prevent unnecessary updates
func normalizeURL(u *url.URL) *url.URL {
	normalized := *u
	normalized.Path = strings.TrimRight(normalized.Path, "/")
	return &normalized
}

// validateFederationRelationship validates the required fields
func validateFederationRelationship(fr *datastore.FederationRelationship, mask *types.FederationRelationshipMask) error {
	if fr == nil {
		return status.Error(codes.InvalidArgument, "federation relationship is required")
	}
	if fr.TrustDomain.IsZero() {
		return status.Error(codes.InvalidArgument, "trust domain is required")
	}

	if mask == nil || mask.BundleEndpointUrl {
		if fr.BundleEndpointURL == nil {
			return status.Error(codes.InvalidArgument, "bundle endpoint URL is required")
		}
		if fr.BundleEndpointURL.Scheme != "https" {
			return status.Error(codes.InvalidArgument, "bundle endpoint URL must use https scheme")
		}
	}

	if mask == nil || mask.BundleEndpointProfile {
		if fr.BundleEndpointProfile == datastore.BundleEndpointSPIFFE {
			if fr.EndpointSPIFFEID.IsZero() {
				return status.Error(codes.InvalidArgument, "endpoint SPIFFE ID is required for https_spiffe profile")
			}
		}
	}

	return nil
}
