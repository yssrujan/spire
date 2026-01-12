package cassandrastore

import (
	"context"

	"github.com/gocql/gocql"
)

// initializeSchema creates the schema based on the actual usage in the codebase
func (ds *Plugin) initializeSchema(ctx context.Context, session *gocql.Session) error {
	schema := []string{
		// ================= BUNDLES =================
		`CREATE TABLE IF NOT EXISTS bundles (
			trust_domain text PRIMARY KEY,
			data blob,
			created_at timestamp,
			updated_at timestamp
		)`,

		// ================= CA JOURNALS =================
		`CREATE TABLE IF NOT EXISTS ca_journals (
			active_x509_authority_id text PRIMARY KEY,
			data blob,
			created_at timestamp,
			updated_at timestamp
		)`,

		// ================= JOIN TOKENS =================
		`CREATE TABLE IF NOT EXISTS join_tokens (
			token_value text PRIMARY KEY,
			expiry timestamp,
			created_at timestamp
		)`,

		// ================= FEDERATION =================
		`CREATE TABLE IF NOT EXISTS federated_trust_domains (
			trust_domain text PRIMARY KEY,
			bundle_endpoint_url text,
			bundle_endpoint_profile text,
			endpoint_spiffe_id text,
			implicit boolean,
			version bigint,
			created_at timestamp,
			updated_at timestamp
		)`,

		`CREATE TABLE IF NOT EXISTS entries_federated_with (
			entry_id text,
			trust_domain text,
			created_at timestamp,
			PRIMARY KEY (entry_id, trust_domain)
		) WITH CLUSTERING ORDER BY (trust_domain ASC)`,

		`CREATE TABLE IF NOT EXISTS entries_by_federated_td (
			trust_domain text,
			entry_id text,
			spiffe_id text,
			PRIMARY KEY (trust_domain, entry_id)
		) WITH CLUSTERING ORDER BY (entry_id ASC)`,

		// ================= ATTESTED NODES =================
		`CREATE TABLE IF NOT EXISTS attested_nodes (
			spiffe_id text PRIMARY KEY,
			data_type text,
			serial_number text,
			expires_at timestamp,
			new_serial_number text,
			new_expires_at timestamp,
			can_reattest boolean,
			selectors set<text>,
			created_at timestamp,
			updated_at timestamp
		)`,

		`CREATE TABLE IF NOT EXISTS attested_nodes_by_expiry (
			expiry_bucket text,
			expires_at timestamp,
			spiffe_id text,
			data_type text,
			serial_number text,
			can_reattest boolean,
			PRIMARY KEY (expiry_bucket, expires_at, spiffe_id)
		)`,

		// ================= ATTESTED NODE EVENTS =================
		`CREATE TABLE IF NOT EXISTS attested_node_events (
			spiffe_id text,
			event_id timeuuid,
			created_at timestamp,
			PRIMARY KEY (spiffe_id, event_id)
		) WITH CLUSTERING ORDER BY (event_id DESC)`,

		`CREATE TABLE IF NOT EXISTS attested_node_events_by_time (
			bucket int,
			event_id timeuuid,
			spiffe_id text,
			PRIMARY KEY (bucket, event_id)
		) WITH CLUSTERING ORDER BY (event_id ASC)`,

		// ================= REGISTRATION ENTRIES =================
		`CREATE TABLE IF NOT EXISTS registered_entries (
			entry_id text PRIMARY KEY,
			spiffe_id text,
			parent_id text,
			ttl int,
			jwt_svid_ttl int,
			admin boolean,
			downstream boolean,
			expiry bigint,
			revision_number bigint,
			store_svid boolean,
			hint text,
			selectors set<text>,
			dns_names set<text>,
			federated_trust_domains set<text>,
			created_at timestamp,
			updated_at timestamp
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_spiffe (
			spiffe_id text,
			entry_id text,
			parent_id text,
			selectors set<text>,
			created_at timestamp,
			PRIMARY KEY (spiffe_id, entry_id)
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_parent (
			parent_id text,
			entry_id text,
			spiffe_id text,
			selectors set<text>,
			created_at timestamp,
			PRIMARY KEY (parent_id, entry_id)
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_selector (
			selector_type text,
			selector_value text,
			entry_id text,
			spiffe_id text,
			parent_id text,
			all_selectors set<text>,
			PRIMARY KEY ((selector_type, selector_value), entry_id)
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_hint (
			hint text,
			entry_id text,
			spiffe_id text,
			PRIMARY KEY (hint, entry_id)
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_expiry (
			expiry_bucket text,
			expiry bigint,
			entry_id text,
			spiffe_id text,
			PRIMARY KEY (expiry_bucket, expiry, entry_id)
		)`,

		`CREATE TABLE IF NOT EXISTS registered_entries_by_downstream (
			downstream boolean,
			entry_id text,
			spiffe_id text,
			parent_id text,
			PRIMARY KEY (downstream, entry_id)
		)`,

		// ================= REGISTRATION ENTRY EVENTS =================
		`CREATE TABLE IF NOT EXISTS registered_entry_events (
			partition int,
			event_id timeuuid,
			entry_id text,
			PRIMARY KEY (partition, event_id)
		) WITH CLUSTERING ORDER BY (event_id DESC)`,
	}

	for _, stmt := range schema {
		if err := session.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return newCassandraError("schema init failed for statement: %s error: %s", stmt, err)
		}
	}

	ds.log.Info("Cassandra schema initialized successfully using initializeSchema2")
	return nil
}
