package cassandrastore

import (
	"context"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
)

// AppendBundle append bundle contents to the existing bundle (by trust domain). If no existing one is present, create it.
func (ds *Plugin) AppendBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return nil, nil
}

// CountBundles can be used to count all existing bundles.
func (ds *Plugin) CountBundles(ctx context.Context) (count int32, err error) {
	return 0, nil
}

// CreateBundle stores the given bundle
func (ds *Plugin) CreateBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return nil, nil
}

// DeleteBundle deletes the bundle with the matching TrustDomain. Any CACert data passed is ignored.
func (ds *Plugin) DeleteBundle(ctx context.Context, trustDomainID string, mode datastore.DeleteMode) (err error) {
	return nil
}

// FetchBundle returns the bundle matching the specified Trust Domain.
func (ds *Plugin) FetchBundle(ctx context.Context, trustDomainID string) (resp *common.Bundle, err error) {
	return nil, nil
}

// ListBundles can be used to fetch all existing bundles.
func (ds *Plugin) ListBundles(ctx context.Context, req *datastore.ListBundlesRequest) (resp *datastore.ListBundlesResponse, err error) {
	return nil, nil
}

// PruneBundle removes expired certs and keys from a bundle
func (ds *Plugin) PruneBundle(ctx context.Context, trustDomainID string, expiresBefore time.Time) (changed bool, err error) {
	return false, nil
}

// SetBundle sets bundle contents. If no bundle exists for the trust domain, it is created.
func (ds *Plugin) SetBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return nil, nil
}

// UpdateBundle updates an existing bundle with the given CAs. Overwrites any
// existing certificates.
func (ds *Plugin) UpdateBundle(ctx context.Context, b *common.Bundle, mask *common.BundleMask) (bundle *common.Bundle, err error) {
	return nil, nil
}

// TaintX509CAByKey taints an X.509 CA signed using the provided public key
func (ds *Plugin) TaintX509CA(ctx context.Context, trustDoaminID string, subjectKeyIDToTaint string) error {
	return nil
}

// RevokeX509CA removes a Root CA from the bundle
func (ds *Plugin) RevokeX509CA(ctx context.Context, trustDoaminID string, subjectKeyIDToRevoke string) error {
	return nil
}

// TaintJWTKey taints a JWT Authority key
func (ds *Plugin) TaintJWTKey(ctx context.Context, trustDoaminID string, authorityID string) (*common.PublicKey, error) {
	return nil, nil
}

// RevokeJWTAuthority removes JWT key from the bundle
func (ds *Plugin) RevokeJWTKey(ctx context.Context, trustDoaminID string, authorityID string) (*common.PublicKey, error) {
	return nil, nil
}

// CreateAttestedNode stores the given attested node
func (ds *Plugin) CreateAttestedNode(ctx context.Context, node *common.AttestedNode) (attestedNode *common.AttestedNode, err error) {
	return nil, nil
}

// FetchAttestedNode fetches an existing attested node by SPIFFE ID
func (ds *Plugin) FetchAttestedNode(ctx context.Context, spiffeID string) (attestedNode *common.AttestedNode, err error) {
	return nil, nil
}

// CountAttestedNodes counts all attested nodes
func (ds *Plugin) CountAttestedNodes(ctx context.Context, req *datastore.CountAttestedNodesRequest) (count int32, err error) {
	return 0, nil
}

// ListAttestedNodes lists all attested nodes (pagination available)
func (ds *Plugin) ListAttestedNodes(ctx context.Context,
	req *datastore.ListAttestedNodesRequest,
) (resp *datastore.ListAttestedNodesResponse, err error) {
	return nil, nil
}

// UpdateAttestedNode updates the given node's cert serial and expiration.
func (ds *Plugin) UpdateAttestedNode(ctx context.Context, n *common.AttestedNode, mask *common.AttestedNodeMask) (node *common.AttestedNode, err error) {
	return nil, nil
}

// DeleteAttestedNode deletes the given attested node and the associated node selectors.
func (ds *Plugin) DeleteAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error) {
	return nil, nil
}

// PruneAttestedExpiredNodes deletes attested nodes with expiration time further than a given duration in the past.
// Non-reattestable nodes are not deleted by default, and have to be included explicitly by setting
// includeNonReattestable = true. Banned nodes are not deleted.
func (ds *Plugin) PruneAttestedExpiredNodes(ctx context.Context, expiredBefore time.Time, includeNonReattestable bool) error {
	return nil
}

// ListAttestedNodeEvents lists all attested node events
func (ds *Plugin) ListAttestedNodeEvents(ctx context.Context, req *datastore.ListAttestedNodeEventsRequest) (resp *datastore.ListAttestedNodeEventsResponse, err error) {
	return nil, nil
}

// PruneAttestedNodeEvents deletes all attested node events older than a specified duration (i.e. more than 24 hours old)
func (ds *Plugin) PruneAttestedNodeEvents(ctx context.Context, olderThan time.Duration) (err error) {
	return nil
}

// CreateRegistrationEntryEventForTestingForTesting creates an attested node event. Used for unit testing.
func (ds *Plugin) CreateAttestedNodeEventForTesting(ctx context.Context, event *datastore.AttestedNodeEvent) error {
	return nil
}

// DeleteAttestedNodeEventForTesting deletes an attested node event by event ID. Used for unit testing.
func (ds *Plugin) DeleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error {
	return nil
}

// FetchAttestedNodeEvent fetches an existing attested node event by event ID
func (ds *Plugin) FetchAttestedNodeEvent(ctx context.Context, eventID uint) (event *datastore.AttestedNodeEvent, err error) {
	return nil, nil
}

// SetNodeSelectors sets node (agent) selectors by SPIFFE ID, deleting old selectors first
func (ds *Plugin) SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) (err error) {
	return nil
}

// GetNodeSelectors gets node (agent) selectors by SPIFFE ID
func (ds *Plugin) GetNodeSelectors(ctx context.Context, spiffeID string,
	dataConsistency datastore.DataConsistency,
) (selectors []*common.Selector, err error) {
	return nil, nil
}

// ListNodeSelectors gets node (agent) selectors by SPIFFE ID
func (ds *Plugin) ListNodeSelectors(ctx context.Context,
	req *datastore.ListNodeSelectorsRequest,
) (resp *datastore.ListNodeSelectorsResponse, err error) {
	return nil, nil
}

// CreateRegistrationEntry stores the given registration entry
func (ds *Plugin) CreateRegistrationEntry(ctx context.Context,
	entry *common.RegistrationEntry,
) (registrationEntry *common.RegistrationEntry, err error) {
	return nil, nil
}

// CreateOrReturnRegistrationEntry stores the given registration entry. If an
// entry already exists with the same (parentID, spiffeID, selector) tuple,
// that entry is returned instead.
func (ds *Plugin) CreateOrReturnRegistrationEntry(ctx context.Context,
	entry *common.RegistrationEntry,
) (registrationEntry *common.RegistrationEntry, existing bool, err error) {
	return nil, false, nil
}

func (ds *Plugin) createOrReturnRegistrationEntry(ctx context.Context,
	entry *common.RegistrationEntry,
) (registrationEntry *common.RegistrationEntry, existing bool, err error) {
	return nil, false, nil

}

// FetchRegistrationEntry fetches an existing registration by entry ID
func (ds *Plugin) FetchRegistrationEntry(ctx context.Context,
	entryID string,
) (*common.RegistrationEntry, error) {
	return nil, nil
}

// FetchRegistrationEntries fetches existing registrations by entry IDs
func (ds *Plugin) FetchRegistrationEntries(ctx context.Context,
	entryIDs []string,
) (map[string]*common.RegistrationEntry, error) {
	return nil, nil
}

// CountRegistrationEntries counts all registrations (pagination available)
func (ds *Plugin) CountRegistrationEntries(ctx context.Context, req *datastore.CountRegistrationEntriesRequest) (count int32, err error) {
	return 0, nil
}

// ListRegistrationEntries lists all registrations (pagination available)
func (ds *Plugin) ListRegistrationEntries(ctx context.Context,
	req *datastore.ListRegistrationEntriesRequest,
) (resp *datastore.ListRegistrationEntriesResponse, err error) {
	return nil, nil
}

// UpdateRegistrationEntry updates an existing registration entry
func (ds *Plugin) UpdateRegistrationEntry(ctx context.Context, e *common.RegistrationEntry, mask *common.RegistrationEntryMask) (entry *common.RegistrationEntry, err error) {
	return nil, nil
}

// DeleteRegistrationEntry deletes the given registration
func (ds *Plugin) DeleteRegistrationEntry(ctx context.Context,
	entryID string,
) (registrationEntry *common.RegistrationEntry, err error) {
	return nil, nil
}

// PruneRegistrationEntries takes a registration entry message, and deletes all entries which have expired
// before the date in the message
func (ds *Plugin) PruneRegistrationEntries(ctx context.Context, expiresBefore time.Time) (err error) {
	return nil
}

// ListRegistrationEntryEvents lists all registration entry events
func (ds *Plugin) ListRegistrationEntryEvents(ctx context.Context, req *datastore.ListRegistrationEntryEventsRequest) (resp *datastore.ListRegistrationEntryEventsResponse, err error) {
	return nil, nil
}

// PruneRegistrationEntryEvents deletes all registration entry events older than a specified duration (i.e. more than 24 hours old)
func (ds *Plugin) PruneRegistrationEntryEvents(ctx context.Context, olderThan time.Duration) (err error) {
	return nil
}

// CreateRegistrationEntryEventForTesting creates a registration entry event. Used for unit testing.
func (ds *Plugin) CreateRegistrationEntryEventForTesting(ctx context.Context, event *datastore.RegistrationEntryEvent) error {
	return nil
}

// DeleteRegistrationEntryEventForTesting deletes the given registration entry event. Used for unit testing.
func (ds *Plugin) DeleteRegistrationEntryEventForTesting(ctx context.Context, eventID uint) error {
	return nil
}

// FetchRegistrationEntryEvent fetches an existing registration entry event by event ID
func (ds *Plugin) FetchRegistrationEntryEvent(ctx context.Context, eventID uint) (event *datastore.RegistrationEntryEvent, err error) {
	return nil, nil
}

// CreateJoinToken takes a Token message and stores it
func (ds *Plugin) CreateJoinToken(ctx context.Context, token *datastore.JoinToken) (err error) {
	if token == nil || token.Token == "" || token.Expiry.IsZero() {
		return nil
	}

	return nil
}

// FetchJoinToken takes a Token message and returns one, populating the fields
// we have knowledge of
func (ds *Plugin) FetchJoinToken(ctx context.Context, token string) (resp *datastore.JoinToken, err error) {
	return nil, nil
}

// DeleteJoinToken deletes the given join token
func (ds *Plugin) DeleteJoinToken(ctx context.Context, token string) (err error) {
	return nil
}

// PruneJoinTokens takes a Token message, and deletes all tokens which have expired
// before the date in the message
func (ds *Plugin) PruneJoinTokens(ctx context.Context, expiry time.Time) (err error) {
	return nil
}

// CreateFederationRelationship creates a new federation relationship. If the bundle endpoint
// profile is 'https_spiffe' and the given federation relationship contains a bundle, the current
// stored bundle is overridden.
// If no bundle is provided and there is not a previously stored bundle in the datastore, the
// federation relationship is not created.
func (ds *Plugin) CreateFederationRelationship(ctx context.Context, fr *datastore.FederationRelationship) (newFr *datastore.FederationRelationship, err error) {
	return nil, nil
}

// DeleteFederationRelationship deletes the federation relationship to the
// given trust domain. The associated trust bundle is not deleted.
func (ds *Plugin) DeleteFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) error {
	if trustDomain.IsZero() {
		return nil
	}

	return nil
}

// FetchFederationRelationship fetches the federation relationship that matches
// the given trust domain. If the federation relationship is not found, nil is returned.
func (ds *Plugin) FetchFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) (fr *datastore.FederationRelationship, err error) {
	if trustDomain.IsZero() {
		return nil, nil
	}

	return nil, nil
}

// ListFederationRelationships can be used to list all existing federation relationships
func (ds *Plugin) ListFederationRelationships(ctx context.Context, req *datastore.ListFederationRelationshipsRequest) (resp *datastore.ListFederationRelationshipsResponse, err error) {
	return nil, nil
}

// UpdateFederationRelationship updates the given federation relationship.
// Attributes are only updated if the correspondent mask value is set to true.
func (ds *Plugin) UpdateFederationRelationship(ctx context.Context, fr *datastore.FederationRelationship, mask *types.FederationRelationshipMask) (newFr *datastore.FederationRelationship, err error) {
	return nil, nil
}

// SetUseServerTimestamps controls whether server-generated timestamps should be used in the database.
// This is only intended to be used by tests in order to produce deterministic timestamp data,
// since some databases round off timestamp data with lower precision.
func (ds *Plugin) SetUseServerTimestamps(useServerTimestamps bool) {
	ds.useServerTimestamps = useServerTimestamps
}

// FetchCAJournal fetches the CA journal that has the given active X509
// authority domain. If the CA journal is not found, nil is returned.
func (ds *Plugin) FetchCAJournal(ctx context.Context, activeX509AuthorityID string) (caJournal *datastore.CAJournal, err error) {
	return nil, nil
}

// ListCAJournalsForTesting returns all the CA journal records, and is meant to
// be used in tests.
func (ds *Plugin) ListCAJournalsForTesting(ctx context.Context) (caJournals []*datastore.CAJournal, err error) {
	return nil, nil
}

// SetCAJournal sets the content for the specified CA journal. If the CA journal
// does not exist, it is created.
func (p *Plugin) SetCAJournal(ctx context.Context, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	return nil, nil
}

// PruneCAJournals prunes the CA journals that have all of their authorities
// expired.
func (ds *Plugin) PruneCAJournals(ctx context.Context, allAuthoritiesExpireBefore int64) error {
	return nil
}
