package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/wjordan/gossip-mesh/objstore"
	"github.com/wjordan/memberlist-quic/tlsutil"
)

const (
	caKeyPath    = "ca/key.pem"
	caCertPath   = "ca/cert.pem"
	caValidity   = 10 * 365 * 24 * time.Hour // 10 years
	nodeValidity = 365 * 24 * time.Hour      // 1 year
)

// getOrCreateCA retrieves the cluster CA from object storage, or generates
// and stores a new one if none exists. Uses CAS (IfNoneMatch) to handle
// concurrent creation by multiple nodes.
func getOrCreateCA(ctx context.Context, store objstore.ObjectStore) (certPEM, keyPEM []byte, err error) {
	certPEM, _, err = store.Get(ctx, caCertPath)
	if err == nil {
		keyPEM, _, err = store.Get(ctx, caKeyPath)
		if err == nil {
			return certPEM, keyPEM, nil
		}
	}

	if !errors.Is(err, objstore.ErrNotFound) {
		return nil, nil, fmt.Errorf("get CA from store: %w", err)
	}

	// Generate new CA.
	certPEM, keyPEM, err = tlsutil.GenerateCA("gossip-mesh", caValidity)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA: %w", err)
	}

	// Store CA cert and key. Use IfNoneMatch to avoid overwriting if another
	// node raced us.
	if _, err := store.Put(ctx, caCertPath, certPEM, objstore.IfNoneMatch()); err != nil {
		if errors.Is(err, objstore.ErrPreconditionFailed) {
			// Another node created the CA first — fetch it.
			return getOrCreateCA(ctx, store)
		}
		return nil, nil, fmt.Errorf("store CA cert: %w", err)
	}
	if _, err := store.Put(ctx, caKeyPath, keyPEM, objstore.IfNoneMatch()); err != nil {
		if errors.Is(err, objstore.ErrPreconditionFailed) {
			return getOrCreateCA(ctx, store)
		}
		return nil, nil, fmt.Errorf("store CA key: %w", err)
	}

	return certPEM, keyPEM, nil
}

// SetupClusterTLS creates a mutual TLS config for the node, using the cluster
// CA from object storage. The provided IPs are included as IP SANs so that
// peers connecting by IP address pass TLS verification.
func SetupClusterTLS(ctx context.Context, store objstore.ObjectStore, nodeID string, ips []net.IP) (*tls.Config, error) {
	if store == nil {
		return nil, fmt.Errorf("ObjectStore required for TLS setup")
	}

	caCert, caKey, err := getOrCreateCA(ctx, store)
	if err != nil {
		return nil, err
	}

	nodeCert, nodeKey, err := tlsutil.GenerateNodeCertWithIPs(caCert, caKey, nodeID, ips, nodeValidity)
	if err != nil {
		return nil, fmt.Errorf("generate node cert: %w", err)
	}

	tlsCfg, err := tlsutil.MutualTLSConfig(nodeCert, nodeKey, caCert)
	if err != nil {
		return nil, fmt.Errorf("create mutual TLS config: %w", err)
	}

	return tlsCfg, nil
}

// GetCACert retrieves the CA certificate PEM and key PEM from the store.
// Useful for regenerating node certificates after learning the public IP.
func GetCACert(ctx context.Context, store objstore.ObjectStore) (certPEM, keyPEM []byte, err error) {
	return getOrCreateCA(ctx, store)
}
