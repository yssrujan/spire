package cassandrastore

import (
	"context"
	"errors"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AppendBundle append bundle contents to the existing bundle (by trust domain). If no existing one is present, create it.
func (ds *Plugin) AppendBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return appendBundle(ctx, ds.session, b, ds.log)
}

// CountBundles can be used to count all existing bundles.
func (ds *Plugin) CountBundles(ctx context.Context) (count int32, err error) {
	return countBundles(ctx, ds.session)
}

// CreateBundle stores the given bundle
func (ds *Plugin) CreateBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return createBundle(ctx, ds.session, b)
}

// DeleteBundle deletes the bundle with the matching TrustDomain. Any CACert data passed is ignored.
func (ds *Plugin) DeleteBundle(ctx context.Context, trustDomainID string, mode datastore.DeleteMode) (err error) {
	return deleteBundle(ctx, ds.session, trustDomainID, mode)
}

// FetchBundle returns the bundle matching the specified Trust Domain.
func (ds *Plugin) FetchBundle(ctx context.Context, trustDomainID string) (resp *common.Bundle, err error) {
	return fetchBundle(ctx, ds.session, trustDomainID)
}

// ListBundles can be used to fetch all existing bundles.
func (ds *Plugin) ListBundles(ctx context.Context, req *datastore.ListBundlesRequest) (resp *datastore.ListBundlesResponse, err error) {
	return listBundles(ctx, ds.session, req)
}

// PruneBundle removes expired certs and keys from a bundle
func (ds *Plugin) PruneBundle(ctx context.Context, trustDomainID string, expiresBefore time.Time) (changed bool, err error) {
	return pruneBundle(ctx, ds.session, trustDomainID, expiresBefore, ds.log)
}

// SetBundle sets bundle contents. If no bundle exists for the trust domain, it is created.
func (ds *Plugin) SetBundle(ctx context.Context, b *common.Bundle) (bundle *common.Bundle, err error) {
	return setBundle(ctx, ds.session, b)
}

// UpdateBundle updates an existing bundle with the given CAs. Overwrites any existing certificates.
func (ds *Plugin) UpdateBundle(ctx context.Context, b *common.Bundle, mask *common.BundleMask) (bundle *common.Bundle, err error) {
	return updateBundle(ctx, ds.session, b, mask)
}

// TaintX509CAByKey taints an X.509 CA signed using the provided public key
func (ds *Plugin) TaintX509CA(ctx context.Context, trustDoaminID string, subjectKeyIDToTaint string) error {
	return taintX509CA(ctx, ds.session, trustDoaminID, subjectKeyIDToTaint)
}

// RevokeX509CA removes a Root CA from the bundle
func (ds *Plugin) RevokeX509CA(ctx context.Context, trustDoaminID string, subjectKeyIDToRevoke string) error {
	return revokeX509CA(ctx, ds.session, trustDoaminID, subjectKeyIDToRevoke)
}

// TaintJWTKey taints a JWT Authority key
func (ds *Plugin) TaintJWTKey(ctx context.Context, trustDoaminID string, authorityID string) (*common.PublicKey, error) {
	return taintJWTKey(ctx, ds.session, trustDoaminID, authorityID)
}

// RevokeJWTAuthority removes JWT key from the bundle
func (ds *Plugin) RevokeJWTKey(ctx context.Context, trustDoaminID string, authorityID string) (*common.PublicKey, error) {
	return revokeJWTKey(ctx, ds.session, trustDoaminID, authorityID)
}

// CreateAttestedNode stores the given attested node
func (ds *Plugin) CreateAttestedNode(ctx context.Context, node *common.AttestedNode) (attestedNode *common.AttestedNode, err error) {
	if node == nil {
		return nil, newCassandraError("invalid request: missing attested node")
	}

	return createAttestedNode(ctx, ds.session, node, ds.log)
}

// FetchAttestedNode fetches an existing attested node by SPIFFE ID
func (ds *Plugin) FetchAttestedNode(ctx context.Context, spiffeID string) (attestedNode *common.AttestedNode, err error) {
	return fetchAttestedNode(ctx, ds.session, spiffeID)
}

// CountAttestedNodes counts all attested nodes
func (ds *Plugin) CountAttestedNodes(ctx context.Context, req *datastore.CountAttestedNodesRequest) (count int32, err error) {
	return countAttestedNodes(ctx, ds.session, req)
}

// ListAttestedNodes lists all attested nodes (pagination available)
func (ds *Plugin) ListAttestedNodes(ctx context.Context,
	req *datastore.ListAttestedNodesRequest,
) (resp *datastore.ListAttestedNodesResponse, err error) {
	return listAttestedNodes(ctx, ds.session, req)
}

// UpdateAttestedNode updates the given node's cert serial and expiration.
func (ds *Plugin) UpdateAttestedNode(ctx context.Context, n *common.AttestedNode, mask *common.AttestedNodeMask) (node *common.AttestedNode, err error) {
	return updateAttestedNode(ctx, ds.session, n, mask, ds.log)
}

// DeleteAttestedNode deletes the given attested node and the associated node selectors.
func (ds *Plugin) DeleteAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error) {
	return deleteAttestedNode(ctx, ds.session, spiffeID, ds.log)
}

// PruneAttestedExpiredNodes deletes attested nodes with expiration time further than a given duration in the past.
func (ds *Plugin) PruneAttestedExpiredNodes(ctx context.Context, expiredBefore time.Time, includeNonReattestable bool) error {
	return pruneAttestedExpiredNodes(ctx, ds.session, expiredBefore, includeNonReattestable, ds.log)
}

// ListAttestedNodeEvents lists all attested node events
func (ds *Plugin) ListAttestedNodeEvents(ctx context.Context, req *datastore.ListAttestedNodeEventsRequest) (resp *datastore.ListAttestedNodeEventsResponse, err error) {
	return listAttestedNodeEvents(ctx, ds.session, req)
}

// PruneAttestedNodeEvents deletes all attested node events older than a specified duration (i.e. more than 24 hours old)
func (ds *Plugin) PruneAttestedNodeEvents(ctx context.Context, olderThan time.Duration) (err error) {
	return pruneAttestedNodeEvents(ctx, ds.session, olderThan)
}

// CreateRegistrationEntryEventForTestingForTesting creates an attested node event. Used for unit testing.
func (ds *Plugin) CreateAttestedNodeEventForTesting(ctx context.Context, event *datastore.AttestedNodeEvent) error {
	return createAttestedNodeEvent(ctx, ds.session, event.SpiffeID)
}

// DeleteAttestedNodeEventForTesting deletes an attested node event by event ID. Used for unit testing.
func (ds *Plugin) DeleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error {
	return deleteAttestedNodeEvent(ctx, ds.session, eventID)
}

// FetchAttestedNodeEvent fetches an existing attested node event by event ID
func (ds *Plugin) FetchAttestedNodeEvent(ctx context.Context, eventID uint) (event *datastore.AttestedNodeEvent, err error) {
	return fetchAttestedNodeEvent(ctx, ds.session, eventID)
}

// SetNodeSelectors sets node (agent) selectors by SPIFFE ID, deleting old selectors first
func (ds *Plugin) SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) (err error) {
	return setNodeSelectors(ctx, ds.session, spiffeID, selectors, ds.log)
}

// GetNodeSelectors gets node (agent) selectors by SPIFFE ID
func (ds *Plugin) GetNodeSelectors(ctx context.Context, spiffeID string,
	dataConsistency datastore.DataConsistency,
) (selectors []*common.Selector, err error) {
	return getNodeSelectors(ctx, ds.session, spiffeID, dataConsistency)
}

// ListNodeSelectors gets node (agent) selectors by SPIFFE ID
func (ds *Plugin) ListNodeSelectors(ctx context.Context,
	req *datastore.ListNodeSelectorsRequest,
) (resp *datastore.ListNodeSelectorsResponse, err error) {
	return listNodeSelectors(ctx, ds.session, req)
}

// CreateRegistrationEntry stores the given registration entry
func (ds *Plugin) CreateRegistrationEntry(ctx context.Context,
	entry *common.RegistrationEntry,
) (registrationEntry *common.RegistrationEntry, err error) {
	out, _, err := createOrReturnRegistrationEntry(ctx, ds.session, entry, ds.log)
	return out, err
}

// CreateOrReturnRegistrationEntry stores the given registration entry. If an
// entry already exists with the same (parentID, spiffeID, selector) tuple,
// that entry is returned instead.
func (ds *Plugin) CreateOrReturnRegistrationEntry(ctx context.Context,
	entry *common.RegistrationEntry,
) (registrationEntry *common.RegistrationEntry, existing bool, err error) {
	return createOrReturnRegistrationEntry(ctx, ds.session, entry, ds.log)
}

// FetchRegistrationEntry fetches an existing registration by entry ID
func (ds *Plugin) FetchRegistrationEntry(ctx context.Context,
	entryID string,
) (*common.RegistrationEntry, error) {
	entries, err := fetchRegistrationEntries(ctx, ds.session, []string{entryID})
	if err != nil {
		return nil, err
	}

	return entries[entryID], nil
}

// FetchRegistrationEntries fetches existing registrations by entry IDs
func (ds *Plugin) FetchRegistrationEntries(ctx context.Context,
	entryIDs []string,
) (map[string]*common.RegistrationEntry, error) {
	return fetchRegistrationEntries(ctx, ds.session, entryIDs)
}

// CountRegistrationEntries counts all registrations (pagination available)
func (ds *Plugin) CountRegistrationEntries(ctx context.Context, req *datastore.CountRegistrationEntriesRequest) (count int32, err error) {
	return countRegistrationEntries(ctx, ds.session, req)
}

// ListRegistrationEntries lists all registrations (pagination available)
func (ds *Plugin) ListRegistrationEntries(ctx context.Context,
	req *datastore.ListRegistrationEntriesRequest,
) (resp *datastore.ListRegistrationEntriesResponse, err error) {
	return listRegistrationEntries(ctx, ds.session, req)
}

// UpdateRegistrationEntry updates an existing registration entry
func (ds *Plugin) UpdateRegistrationEntry(ctx context.Context, e *common.RegistrationEntry, mask *common.RegistrationEntryMask) (entry *common.RegistrationEntry, err error) {
	return updateRegistrationEntry(ctx, ds.session, e, mask, ds.log)
}

// DeleteRegistrationEntry deletes the given registration
func (ds *Plugin) DeleteRegistrationEntry(ctx context.Context,
	entryID string,
) (registrationEntry *common.RegistrationEntry, err error) {
	return deleteRegistrationEntry(ctx, ds.session, entryID)
}

// PruneRegistrationEntries takes a registration entry message, and deletes all entries which have expired
// before the date in the message
func (ds *Plugin) PruneRegistrationEntries(ctx context.Context, expiresBefore time.Time) (err error) {
	return pruneRegistrationEntries(ctx, ds.session, expiresBefore, ds.log)
}

// ListRegistrationEntryEvents lists all registration entry events
func (ds *Plugin) ListRegistrationEntryEvents(ctx context.Context, req *datastore.ListRegistrationEntryEventsRequest) (resp *datastore.ListRegistrationEntryEventsResponse, err error) {
	return listRegistrationEntryEvents(ctx, ds.session, req)
}

// PruneRegistrationEntryEvents deletes all registration entry events older than a specified duration (i.e. more than 24 hours old)
func (ds *Plugin) PruneRegistrationEntryEvents(ctx context.Context, olderThan time.Duration) (err error) {
	return pruneRegistrationEntryEvents(ctx, ds.session, olderThan)
}

// CreateRegistrationEntryEventForTesting creates a registration entry event. Used for unit testing.
func (ds *Plugin) CreateRegistrationEntryEventForTesting(ctx context.Context, event *datastore.RegistrationEntryEvent) error {
	return createRegistrationEntryEvent(ctx, ds.session, event.EntryID)
}

// DeleteRegistrationEntryEventForTesting deletes the given registration entry event. Used for unit testing.
func (ds *Plugin) DeleteRegistrationEntryEventForTesting(ctx context.Context, eventID uint) error {
	return deleteRegistrationEntryEventForTesting(ctx, ds.session, eventID)
}

// FetchRegistrationEntryEvent fetches an existing registration entry event by event ID
func (ds *Plugin) FetchRegistrationEntryEvent(ctx context.Context, eventID uint) (event *datastore.RegistrationEntryEvent, err error) {
	return fetchRegistrationEntryEvent(ctx, ds.session, eventID)
}

// CreateJoinToken takes a Token message and stores it
func (ds *Plugin) CreateJoinToken(ctx context.Context, token *datastore.JoinToken) (err error) {
	if token == nil || token.Token == "" || token.Expiry.IsZero() {
		return errors.New("token and expiry are required")
	}

	return createJoinToken(ctx, ds.session, token)
}

// FetchJoinToken takes a Token message and returns one, populating the fields
// we have knowledge of
func (ds *Plugin) FetchJoinToken(ctx context.Context, token string) (resp *datastore.JoinToken, err error) {
	return fetchJoinToken(ctx, ds.session, token)
}

// DeleteJoinToken deletes the given join token
func (ds *Plugin) DeleteJoinToken(ctx context.Context, token string) (err error) {
	return deleteJoinToken(ctx, ds.session, token)
}

// PruneJoinTokens takes a Token message, and deletes all tokens which have expired
// before the date in the message
func (ds *Plugin) PruneJoinTokens(ctx context.Context, expiry time.Time) (err error) {
	return pruneJoinTokens(ctx, ds.session, expiry)
}

// CreateFederationRelationship creates a new federation relationship. If the bundle endpoint
// profile is 'https_spiffe' and the given federation relationship contains a bundle, the current
// stored bundle is overridden.
// If no bundle is provided and there is not a previously stored bundle in the datastore, the
// federation relationship is not created.
func (ds *Plugin) CreateFederationRelationship(ctx context.Context, fr *datastore.FederationRelationship) (newFr *datastore.FederationRelationship, err error) {
	if err := validateFederationRelationship(fr, protoutil.AllTrueFederationRelationshipMask); err != nil {
		return nil, err
	}

	return createFederationRelationship(ctx, ds.session, fr)
}

// DeleteFederationRelationship deletes the federation relationship to the
// given trust domain. The associated trust bundle is not deleted.
func (ds *Plugin) DeleteFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) error {
	if trustDomain.IsZero() {
		return status.Error(codes.InvalidArgument, "trust domain is required")
	}

	return deleteFederationRelationship(ctx, ds.session, trustDomain)
}

// FetchFederationRelationship fetches the federation relationship that matches
// the given trust domain. If the federation relationship is not found, nil is returned.
func (ds *Plugin) FetchFederationRelationship(ctx context.Context, trustDomain spiffeid.TrustDomain) (fr *datastore.FederationRelationship, err error) {
	if trustDomain.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "trust domain is required")
	}

	return fetchFederationRelationship(ctx, ds.session, trustDomain)
}

// ListFederationRelationships can be used to list all existing federation relationships
func (ds *Plugin) ListFederationRelationships(ctx context.Context, req *datastore.ListFederationRelationshipsRequest) (resp *datastore.ListFederationRelationshipsResponse, err error) {
	return listFederationRelationships(ctx, ds.session, req, ds.log)
}

// UpdateFederationRelationship updates the given federation relationship.
// Attributes are only updated if the correspondent mask value is set to true.
func (ds *Plugin) UpdateFederationRelationship(ctx context.Context, fr *datastore.FederationRelationship, mask *types.FederationRelationshipMask) (newFr *datastore.FederationRelationship, err error) {
	if err := validateFederationRelationship(fr, mask); err != nil {
		return nil, err
	}

	return updateFederationRelationship(ctx, ds.session, fr, mask)
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
	if activeX509AuthorityID == "" {
		return nil, status.Error(codes.InvalidArgument, "active X509 authority ID is required")
	}

	return fetchCAJournal(ctx, ds.session, activeX509AuthorityID)
}

// ListCAJournalsForTesting returns all the CA journal records, and is meant to
// be used in tests.
func (ds *Plugin) ListCAJournalsForTesting(ctx context.Context) (caJournals []*datastore.CAJournal, err error) {
	return listCAJournalsForTesting(ctx, ds.session)
}

// SetCAJournal sets the content for the specified CA journal. If the CA journal
// does not exist, it is created.
func (p *Plugin) SetCAJournal(ctx context.Context, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	return setCAJournal(ctx, p.session, caJournal)
}

// PruneCAJournals prunes the CA journals that have all of their authorities
// expired.
func (ds *Plugin) PruneCAJournals(ctx context.Context, allAuthoritiesExpireBefore int64) error {
	return pruneCAJournals(ctx, ds.session, allAuthoritiesExpireBefore)
}
