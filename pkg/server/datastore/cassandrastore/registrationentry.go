package cassandrastore

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/spiffe/spire/proto/spire/common"
)

func deleteRegistrationEntry(ctx context.Context, session *gocql.Session, id string) (*common.RegistrationEntry, error) {
	return nil, nil
}
