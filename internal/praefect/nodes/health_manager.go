package nodes

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// HealthClients contains HealthClients for every physical storage by virtual storage.
type HealthClients map[string]map[string]grpc_health_v1.HealthClient

// HealthManager monitors the health status of the storage cluster. The monitoring frequency
// is controlled by the Ticker passed in to Run method. On each tick, the HealthManager:
//
// 1. Runs health checks on configured physical storages by performing a gRPC call
//    to the health checking endpoint. If an error tracker is configured, it also considers
//    its view of the node's health.
// 2. Stores its health check results in the `node_status` table.
// 3. Checks if the clusters consensus of healthy nodes has changed by querying the `node_status`
//    table for results of the other Praefect instances. If so, it sends to the Updated channel
//    to signal a change in the cluster status.
//
// To determine the participants for the quorum, we use a lightweight service discovery protocol.
// A Praefect instance is deemed to be voting member if it has a recent health check in the
// `node_status` table. Each Praefect node is identified by their host name and the provided
// stable ID. The stable ID should uniquely identify a Praefect instance on the host.
type HealthManager struct {
	log         logrus.FieldLogger
	db          glsql.Querier
	handleError func(error) error
	// clients contains connections to the configured physical storages within each
	// virtual storage.
	clients HealthClients
	// praefectName is the identifier of the Praefect running the HealthManager. It should
	// be stable through the restarts as they are used to identify quorum members.
	praefectName string
	// healthCheckTimeout is the duration after a health check attempt times out.
	healthCheckTimeout time.Duration
	// databaseTimeout applies the timeout for the database update. It returns a context with
	// the timeout applied and a cancellation function. This should be shorter than the failover
	// timeout, otherwise it is possible that the updated health checks are immediately considered
	// outdated after the update has finished. This can be difficult to debug as Gitaly nodes are
	// seemingly responding to the health checks but are considered outdated by Praefect.
	databaseTimeout func(context.Context) (context.Context, func())
	firstUpdate     bool
	updated         chan struct{}

	locallyHealthy atomic.Value
}

// NewHealthManager returns a new health manager that monitors which nodes in the cluster
// are healthy.
func NewHealthManager(
	log logrus.FieldLogger,
	db glsql.Querier,
	praefectName string,
	clients HealthClients,
) *HealthManager {
	log = log.WithField("component", "HealthManager")
	hm := HealthManager{
		log:     log,
		db:      db,
		clients: clients,
		handleError: func(err error) error {
			log.WithError(err).Error("checking health failed")
			return nil
		},
		praefectName:       praefectName,
		healthCheckTimeout: healthcheckTimeout,
		databaseTimeout: func(ctx context.Context) (context.Context, func()) {
			return context.WithTimeout(ctx, 5*time.Second)
		},
		firstUpdate: true,
		updated:     make(chan struct{}, 1),
	}

	hm.locallyHealthy.Store(make(map[string][]string, len(clients)))

	return &hm
}

// Run runs the health check on every tick by the Ticker until the context is
// canceled. Returns the error from the context.
func (hm *HealthManager) Run(ctx context.Context, ticker helper.Ticker) error {
	hm.log.Info("health manager started")
	defer hm.log.Info("health manager stopped")

	defer ticker.Stop()

	for {
		ticker.Reset()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			virtualStorages, physicalStorages, healthy := hm.performHealthChecks(ctx)
			if err := hm.updateHealthChecks(ctx, virtualStorages, physicalStorages, healthy); err != nil {
				if err := hm.handleError(err); err != nil {
					return err
				}
			}
		}
	}
}

// Updated returns a channel that is sent to when the set of healthy nodes is updated.
// Update is also sent to on the first check even if no nodes are healthy. The channel
// is buffered to allow HealthManager to proceed with cluster health monitoring when
// the channel consumer is slow.
func (hm *HealthManager) Updated() <-chan struct{} {
	return hm.updated
}

// HealthyNodes returns a map of healthy nodes in each virtual storage as seen by the latest
// local health check.
func (hm *HealthManager) HealthyNodes() map[string][]string {
	return hm.locallyHealthy.Load().(map[string][]string)
}

func (hm *HealthManager) updateHealthChecks(ctx context.Context, virtualStorages, physicalStorages []string, healthy []bool) error {
	locallyHealthy := map[string][]string{}
	for i := range virtualStorages {
		if !healthy[i] {
			continue
		}

		locallyHealthy[virtualStorages[i]] = append(locallyHealthy[virtualStorages[i]], physicalStorages[i])
	}

	hm.locallyHealthy.Store(locallyHealthy)

	ctx, cancel := hm.databaseTimeout(ctx)
	defer cancel()

	if _, err := hm.db.ExecContext(ctx, `
INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at, last_seen_active_at)
SELECT $1, shard_name, node_name, NOW(), CASE WHEN is_healthy THEN NOW() ELSE NULL END
FROM (
    SELECT unnest($2::text[]) AS shard_name,
           unnest($3::text[]) AS node_name,
           unnest($4::boolean[]) AS is_healthy
    ORDER BY shard_name, node_name
) AS results
ON CONFLICT (praefect_name, shard_name, node_name)
	DO UPDATE SET
		last_contact_attempt_at = NOW(),
		last_seen_active_at = COALESCE(EXCLUDED.last_seen_active_at, node_status.last_seen_active_at)
	`,
		hm.praefectName,
		pq.StringArray(virtualStorages),
		pq.StringArray(physicalStorages),
		pq.BoolArray(healthy),
	); err != nil {
		return fmt.Errorf("update checks: %w", err)
	}

	if hm.firstUpdate {
		hm.firstUpdate = false
		hm.updated <- struct{}{}
	}

	return nil
}

func (hm *HealthManager) performHealthChecks(ctx context.Context) ([]string, []string, []bool) {
	nodeCount := 0
	for _, physicalStorages := range hm.clients {
		nodeCount += len(physicalStorages)
	}

	virtualStorages := make([]string, nodeCount)
	physicalStorages := make([]string, nodeCount)
	healthy := make([]bool, nodeCount)

	var wg sync.WaitGroup
	wg.Add(nodeCount)

	ctx, cancel := context.WithTimeout(ctx, hm.healthCheckTimeout)
	defer cancel()

	i := 0
	for virtualStorage, storages := range hm.clients {
		for storage, client := range storages {
			virtualStorages[i] = virtualStorage
			physicalStorages[i] = storage
			go func(i int, client grpc_health_v1.HealthClient) {
				defer wg.Done()

				correlationID := correlation.SafeRandomID()
				ctx := correlation.ContextWithCorrelation(ctx, correlationID)
				resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
				if err != nil {
					hm.log.WithFields(logrus.Fields{
						logrus.ErrorKey:   err,
						"virtual_storage": virtualStorages[i],
						"storage":         physicalStorages[i],
						"correlation_id":  correlationID,
					}).Error("failed checking node health")
				}

				healthy[i] = resp != nil && resp.Status == grpc_health_v1.HealthCheckResponse_SERVING
			}(i, client)
			i++
		}
	}

	wg.Wait()

	return virtualStorages, physicalStorages, healthy
}
