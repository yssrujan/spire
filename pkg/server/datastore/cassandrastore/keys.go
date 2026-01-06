package cassandrastore

// Done

import (
	"context"
	"crypto/x509"

	"github.com/gocql/gocql"
	"github.com/gogo/status"
	"github.com/spiffe/spire/pkg/common/x509util"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
)

func taintX509CA(ctx context.Context, session *gocql.Session, trustDomainID string, subjectKeyIDToTaint string) error {
	if trustDomainID == "" {
		return status.Error(codes.InvalidArgument, "missing trust domain ID")
	}
	if subjectKeyIDToTaint == "" {
		return status.Error(codes.InvalidArgument, "missing subject key ID")
	}

	bundle, err := fetchBundle(ctx, session, trustDomainID)
	if err != nil {
		return err
	}
	if bundle == nil {
		return status.Error(codes.NotFound, "bundle not found")
	}

	found := false
	for _, cert := range bundle.RootCas {
		x509Cert, err := x509.ParseCertificate(cert.DerBytes)
		if err != nil {
			continue
		}

		subjectKeyID := x509util.SubjectKeyIDToString(x509Cert.SubjectKeyId)
		if subjectKeyID == subjectKeyIDToTaint {
			if cert.TaintedKey {
				return status.Error(codes.InvalidArgument, "root CA is already tainted")
			}

			cert.TaintedKey = true
			found = true
			break
		}
	}

	if !found {
		return status.Error(codes.NotFound, "no ca found with provided subject key ID")
	}

	bundle.SequenceNumber++

	if _, err := setBundle(ctx, session, bundle); err != nil {
		return err
	}

	return nil
}

func revokeX509CA(ctx context.Context, session *gocql.Session, trustDomainID string, subjectKeyIDToRevoke string) error {
	if trustDomainID == "" {
		return status.Error(codes.InvalidArgument, "missing trust domain ID")
	}
	if subjectKeyIDToRevoke == "" {
		return status.Error(codes.InvalidArgument, "missing subject key ID")
	}

	bundle, err := fetchBundle(ctx, session, trustDomainID)
	if err != nil {
		return err
	}
	if bundle == nil {
		return status.Error(codes.NotFound, "bundle not found")
	}

	found := false
	newRootCAs := []*common.Certificate{}

	for _, cert := range bundle.RootCas {
		x509Cert, err := x509.ParseCertificate(cert.DerBytes)
		if err != nil {
			newRootCAs = append(newRootCAs, cert)
			continue
		}

		subjectKeyID := x509util.SubjectKeyIDToString(x509Cert.SubjectKeyId)
		if subjectKeyID == subjectKeyIDToRevoke {
			if !cert.TaintedKey {
				return status.Error(codes.InvalidArgument, "it is not possible to revoke an untainted root CA")
			}
			found = true
			continue
		}

		newRootCAs = append(newRootCAs, cert)
	}

	if !found {
		return status.Error(codes.NotFound, "no root CA found with provided subject key ID")
	}

	bundle.RootCas = newRootCAs
	bundle.SequenceNumber++

	if _, err := setBundle(ctx, session, bundle); err != nil {
		return err
	}

	return nil
}

func taintJWTKey(ctx context.Context, session *gocql.Session, trustDomainID string, authorityID string) (*common.PublicKey, error) {
	if trustDomainID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing trust domain ID")
	}
	if authorityID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing authority ID")
	}

	bundle, err := fetchBundle(ctx, session, trustDomainID)
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		return nil, status.Error(codes.NotFound, "bundle not found")
	}

	var taintedKey *common.PublicKey
	for _, key := range bundle.JwtSigningKeys {
		if key.Kid == authorityID {
			if key.TaintedKey {
				return nil, status.Error(codes.InvalidArgument, "key is already tainted")
			}

			if taintedKey != nil {
				return nil, status.Error(codes.Internal, "another JWT Key found with the same KeyID")
			}

			key.TaintedKey = true
			taintedKey = key
		}
	}

	if taintedKey == nil {
		return nil, status.Error(codes.NotFound, "no JWT Key found with provided key ID")
	}

	bundle.SequenceNumber++

	if _, err := setBundle(ctx, session, bundle); err != nil {
		return nil, err
	}

	return taintedKey, nil
}

func revokeJWTKey(ctx context.Context, session *gocql.Session, trustDomainID string, authorityID string) (*common.PublicKey, error) {
	if trustDomainID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing trust domain ID")
	}
	if authorityID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing authority ID")
	}

	bundle, err := fetchBundle(ctx, session, trustDomainID)
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		return nil, status.Error(codes.NotFound, "bundle not found")
	}

	var revokedKey *common.PublicKey
	newKeys := []*common.PublicKey{}

	for _, key := range bundle.JwtSigningKeys {
		if key.Kid == authorityID {
			if !key.TaintedKey {
				return nil, status.Error(codes.InvalidArgument, "it is not possible to revoke an untainted key")
			}

			if revokedKey != nil {
				return nil, status.Error(codes.Internal, "another key found with the same KeyID")
			}

			revokedKey = key
			continue
		}

		newKeys = append(newKeys, key)
	}

	if revokedKey == nil {
		return nil, status.Error(codes.NotFound, "no JWT Key found with provided key ID")
	}

	bundle.JwtSigningKeys = newKeys
	bundle.SequenceNumber++

	if _, err := setBundle(ctx, session, bundle); err != nil {
		return nil, err
	}

	return revokedKey, nil
}
