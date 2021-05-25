package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/metrics"
)

const (
	failoverTimeout       = 10 * time.Second
	activePraefectTimeout = 60 * time.Second
)

type sqlCandidate struct {
	Node
}

// sqlElector manages the primary election for one virtual storage (aka
// shard). It enables multiple, redundant Praefect processes to run,
// which is needed to eliminate a single point of failure in Gitaly High
// Avaiability.
//
// The sqlElector is responsible for:
//
// 1. Monitoring and updating the status of all nodes within the shard.
// 2. Electing a new primary of the shard based on the health.
//
// Every Praefect node periodically performs a health check RPC with a Gitaly node. The health check
// interval is configured via `internal/praefect/config.Failover.MonitorInterval`.
//
// 1. For each node, Praefect updates a row in a new table
// (`node_status`) with the following information:
//
//    a. The name of the Praefect instance (`praefect_name`)
//    b. The name of the virtual storage name (`shard_name`)
//    c. The name of the Gitaly storage name (`storage_name`)
//    d. The timestamp of the last time Praefect tried to reach that node (`last_contact_attempt_at`)
//    e. The timestamp of the last successful health check (`last_seen_active_at`)
//
// 2. Once the health checks are complete, Praefect node does a `SELECT` from
// `node_status` to determine healthy nodes. A healthy node is
// defined by:
//    a. A node that has a recent successful error check (e.g. one in
//    the last 10 s).
//    b. A majority of the available Praefect nodes have entries that
//    match the two above.
//
// To determine the majority, we use a lightweight service discovery
// protocol: a Praefect node is deemed a voting member if the
// `praefect_name` has a recent `last_contact_attempt_at` in the
// `node_status` table. The name is derived from a combination
// of the hostname and listening port/socket.
//
// The primary of each shard is listed in the
// `shard_primaries`. If the current primary is in the healthy
// node list, then sqlElector updates its internal state to match.
//
// Otherwise, if there is no primary or it is unhealthy, any Praefect node
// can elect a new primary by choosing candidate from the healthy node
// list. If there are no candidate nodes, the primary is demoted by setting the `demoted` flag
// in `shard_primaries`.
//
// In case of a failover, the virtual storage is marked as read-only until writes are manually enabled
// again. This status is stored in the `shard_primaries` table's `read_only` column. If `read_only` is
// set, mutator RPCs against the storage shard should be blocked in order to prevent new primary from
// diverging from the previous primary before data recovery attempts have been made.
type sqlElector struct {
	m               sync.RWMutex
	praefectName    string
	shardName       string
	nodes           []*sqlCandidate
	primaryNode     *sqlCandidate
	db              *sql.DB
	log             logrus.FieldLogger
	failoverTimeout time.Duration
}

func newSQLElector(name string, c config.Config, db *sql.DB, log logrus.FieldLogger, ns []*nodeStatus) *sqlElector {
	log = log.WithField("virtual_storage", name)
	praefectName := GeneratePraefectName(c, log)

	log = log.WithField("praefectName", praefectName)
	log.Info("Using SQL election strategy")

	nodes := make([]*sqlCandidate, len(ns))
	for i, n := range ns {
		nodes[i] = &sqlCandidate{Node: n}
	}

	return &sqlElector{
		praefectName:    praefectName,
		shardName:       name,
		db:              db,
		log:             log,
		nodes:           nodes,
		primaryNode:     nodes[0],
		failoverTimeout: failoverTimeout,
	}
}

// GeneratePraefectName generates a name so that each Praefect process
// can report node statuses independently. This will enable us to do a
// SQL election to determine which nodes are active. Ideally this name
// doesn't change across restarts since that may temporarily make it
// look like there are more Praefect processes active for
// determining a quorum.
func GeneratePraefectName(c config.Config, log logrus.FieldLogger) string {
	name, err := os.Hostname()

	if err != nil {
		name = uuid.New().String()
		log.WithError(err).WithField("praefectName", name).Warn("unable to determine Praefect hostname, using randomly generated UUID")
	}

	if c.ListenAddr != "" {
		return fmt.Sprintf("%s:%s", name, c.ListenAddr)
	}

	return fmt.Sprintf("%s:%s", name, c.SocketPath)
}

// start launches a Goroutine to check the state of the nodes and
// continuously monitor their health via gRPC health checks.
func (s *sqlElector) start(bootstrapInterval, monitorInterval time.Duration) {
	s.bootstrap(bootstrapInterval)
	go s.monitor(monitorInterval)
}

func (s *sqlElector) bootstrap(d time.Duration) {
	ctx := context.Background()
	s.checkNodes(ctx)
}

func (s *sqlElector) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	ctx := context.Background()

	for {
		<-ticker.C
		s.checkNodes(ctx)
	}
}

func (s *sqlElector) checkNodes(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.log.WithError(err).Error("unable to begin a database transaction")
		return err
	}

	defer func() {
		if err := tx.Commit(); err != nil {
			s.log.WithError(err).Error("failed committing transaction")
		}
	}()

	var wg sync.WaitGroup

	defer s.updateMetrics()

	for _, n := range s.nodes {
		wg.Add(1)

		go func(n Node) {
			defer wg.Done()
			result, _ := n.CheckHealth(ctx)
			if err := s.updateNode(ctx, tx, n, result); err != nil {
				s.log.WithError(err).WithFields(logrus.Fields{
					"storage": n.GetStorage(),
					"address": n.GetAddress(),
				}).Error("error checking node")
			}
		}(n)
	}

	wg.Wait()

	err = s.validateAndUpdatePrimary(ctx, tx)

	if err != nil {
		s.log.WithError(err).Error("unable to validate primary")
		return err
	}

	// The attempt to elect a primary may have conflicted with another
	// node attempting to elect a primary. We check the database again
	// to see the current state.
	candidate, err := s.lookupPrimary(ctx, tx)
	if err != nil {
		s.log.WithError(err).Error("error looking up primary")
		return err
	}

	s.setPrimary(candidate)
	return nil
}

func (s *sqlElector) setPrimary(candidate *sqlCandidate) {
	s.m.Lock()
	defer s.m.Unlock()

	if candidate != s.primaryNode {
		var oldPrimary string
		var newPrimary string

		if s.primaryNode != nil {
			oldPrimary = s.primaryNode.GetStorage()
		}

		if candidate != nil {
			newPrimary = candidate.GetStorage()
		}

		s.log.WithFields(logrus.Fields{
			"oldPrimary": oldPrimary,
			"newPrimary": newPrimary,
		}).Info("primary node changed")

		s.primaryNode = candidate
	}
}

func (s *sqlElector) updateNode(ctx context.Context, tx *sql.Tx, node Node, result bool) error {
	var q string

	if result {
		q = `INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at, last_seen_active_at)
VALUES ($1, $2, $3, NOW(), NOW())
ON CONFLICT (praefect_name, shard_name, node_name)
DO UPDATE SET
last_contact_attempt_at = NOW(),
last_seen_active_at = NOW()`
	} else {
		// Omit the last_seen_active_at since we weren't successful at contacting this node
		q = `INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (praefect_name, shard_name, node_name)
DO UPDATE SET
last_contact_attempt_at = NOW()`
	}

	_, err := tx.ExecContext(ctx, q, s.praefectName, s.shardName, node.GetStorage())

	if err != nil {
		s.log.Errorf("Error updating node: %s", err)
	}

	return err
}

// GetShard gets the current status of the shard. ErrPrimaryNotHealthy
// is returned if a primary does not exist.
func (s *sqlElector) GetShard(ctx context.Context) (Shard, error) {
	primary, err := s.lookupPrimary(ctx, s.db)
	if err != nil {
		return Shard{}, err
	}

	s.setPrimary(primary)
	if primary == nil {
		return Shard{}, ErrPrimaryNotHealthy
	}

	var secondaries []Node
	for _, n := range s.nodes {
		if primary != n {
			secondaries = append(secondaries, n)
		}
	}

	return Shard{
		Primary:     primary,
		Secondaries: secondaries,
	}, nil
}

func (s *sqlElector) updateMetrics() {
	s.m.RLock()
	primary := s.primaryNode
	s.m.RUnlock()

	for _, node := range s.nodes {
		var val float64

		if primary == node {
			val = 1
		}

		metrics.PrimaryGauge.WithLabelValues(s.shardName, node.GetStorage()).Set(val)
	}
}

func (s *sqlElector) getQuorumCount(ctx context.Context, tx *sql.Tx) (int, error) {
	// This is crude form of service discovery. Find how many active
	// Praefect nodes based on whether they attempted to update entries.
	q := `SELECT COUNT (DISTINCT praefect_name) FROM node_status WHERE shard_name = $1 AND last_contact_attempt_at >= NOW() - INTERVAL '1 MICROSECOND' * $2`

	var totalCount int

	if err := tx.QueryRowContext(ctx, q, s.shardName, activePraefectTimeout.Microseconds()).Scan(&totalCount); err != nil {
		return 0, fmt.Errorf("error retrieving quorum count: %w", err)
	}

	if totalCount <= 1 {
		return 1, nil
	}

	quorumCount := int(math.Ceil(float64(totalCount) / 2))

	return quorumCount, nil
}

func (s *sqlElector) lookupNodeByName(name string) *sqlCandidate {
	for _, n := range s.nodes {
		if n.GetStorage() == name {
			return n
		}
	}

	return nil
}

func nodeInSlice(candidates []*sqlCandidate, node *sqlCandidate) bool {
	for _, n := range candidates {
		if n == node {
			return true
		}
	}

	return false
}

func (s *sqlElector) demotePrimary(ctx context.Context, tx glsql.Querier) error {
	log := s.log
	if s.primaryNode != nil {
		log = s.log.WithField("primary", s.primaryNode.GetStorage())
	}
	log.Info("demoting primary node")

	s.setPrimary(nil)

	q := "UPDATE shard_primaries SET demoted = true WHERE shard_name = $1"
	_, err := tx.ExecContext(ctx, q, s.shardName)

	return err
}

func (s *sqlElector) electNewPrimary(ctx context.Context, tx *sql.Tx, candidates []*sqlCandidate) error {
	if len(candidates) == 0 {
		return errors.New("candidates cannot be empty")
	}

	candidateStorages := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		candidateStorages = append(candidateStorages, candidate.GetStorage())
	}

	q := `
		SELECT storages.storage
		FROM repositories AS r
		CROSS JOIN (SELECT UNNEST($1::TEXT[]) AS storage) AS storages
		LEFT JOIN storage_repositories AS sr USING(virtual_storage, relative_path, storage)
		WHERE r.virtual_storage = $2
		GROUP BY storages.storage
		ORDER BY SUM(r.generation - COALESCE(sr.generation, -1))
		LIMIT 1`

	var newPrimaryStorage string
	var fallbackChoice bool
	if err := tx.QueryRowContext(ctx, q, pq.StringArray(candidateStorages), s.shardName).Scan(&newPrimaryStorage); err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("retrieve potential candidate: %w", err)
		}

		// the state of the repositories is undefined - use first candidate
		newPrimaryStorage = candidateStorages[0]
		fallbackChoice = true
	}

	s.log.WithFields(logrus.Fields{
		"candidates":      candidateStorages,
		"new_primary":     newPrimaryStorage,
		"fallback_choice": fallbackChoice,
	}).Info("new primary selected")

	// read_only is set only when a row already exists in the table. This avoids new shards, which
	// do not yet have a row in the table, from starting in read-only mode. In a failover scenario,
	// a row already exists in the table denoting the previous primary, and thus the shard should
	// be switched to read-only mode.
	//
	// Previous write-enabled primary is stored in `previous_writable_primary` column. We store it to determine
	// unreplicated writes from the previous write-enabled primary to the current primary to report and
	// automatically repair data loss cases. Read-only primaries are not stored, as they can't receive
	// writes that could fail to be replicated to other nodes. Consider the failover scenarios:
	//     N1 (RW) -> N2 (RO) -> N1 (RO): `previous_writable_primary` remains N1 as N1 was the only write-enabled primary
	//                                     and thus has all the possible writes
	//     N1 (RW) -> N2 (RW) -> N1 (RO): `previous_writable_primary` is N2 as we only store the previous write-enabled
	//                                     primary. If writes are enable on shard with data loss, the data loss
	//                                     is considered acknowledged.
	//     N1 (RO) -> N2 (RW)           : `previous_writable_primary` is null as there could not have been unreplicated
	//                                     writes from the read-only primary N1
	//     N1 (RW) -> N2 (RW)           : `previous_writable_primary` is N1 as it might have had unreplicated writes when
	//                                     N2 got elected
	//     N1 (RW) -> N2 (RO) -> N3 (RW): `previous_writable_primary` is N1 as N2 was not write-enabled before the second
	//                                    failover and thus any data missing from N3 must be on N1.
	q = `INSERT INTO shard_primaries (elected_by_praefect, shard_name, node_name, elected_at)
	SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR, NOW()
	WHERE $3 != COALESCE((SELECT node_name FROM shard_primaries WHERE shard_name = $2::VARCHAR AND demoted = false), '')
	ON CONFLICT (shard_name)
	DO UPDATE SET elected_by_praefect = EXCLUDED.elected_by_praefect
				, node_name = EXCLUDED.node_name
				, elected_at = EXCLUDED.elected_at
				, demoted = false
	   WHERE shard_primaries.elected_at < now() - INTERVAL '1 MICROSECOND' * $4
	`
	_, err := tx.ExecContext(ctx, q, s.praefectName, s.shardName, newPrimaryStorage, s.failoverTimeout.Microseconds())
	if err != nil {
		s.log.Errorf("error updating new primary: %s", err)
		return err
	}

	return nil
}

func (s *sqlElector) validateAndUpdatePrimary(ctx context.Context, tx *sql.Tx) error {
	quorumCount, err := s.getQuorumCount(ctx, tx)

	if err != nil {
		return err
	}

	// Retrieves candidates, ranked by the ones that are the most active
	q := `SELECT node_name FROM node_status
			WHERE shard_name = $1 AND last_seen_active_at >= NOW() - INTERVAL '1 MICROSECOND' * $2
			GROUP BY node_name
			HAVING COUNT(praefect_name) >= $3
			ORDER BY COUNT(node_name) DESC, node_name ASC`

	rows, err := tx.QueryContext(ctx, q, s.shardName, s.failoverTimeout.Microseconds(), quorumCount)
	if err != nil {
		return fmt.Errorf("error retrieving candidates: %w", err)
	}
	defer rows.Close()

	var candidates []*sqlCandidate

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("error retrieving candidate rows: %w", err)
		}

		node := s.lookupNodeByName(name)

		if node != nil {
			candidates = append(candidates, node)
		} else {
			s.log.Errorf("unknown candidate node name found: %s", name)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// Check if primary is in this list
	primaryNode, err := s.lookupPrimary(ctx, tx)
	if err != nil {
		s.log.WithError(err).Error("error looking up primary")
		return err
	}

	if len(candidates) == 0 {
		return s.demotePrimary(ctx, tx)
	}

	if primaryNode == nil || !nodeInSlice(candidates, primaryNode) {
		return s.electNewPrimary(ctx, tx, candidates)
	}

	return nil
}

func (s *sqlElector) lookupPrimary(ctx context.Context, tx glsql.Querier) (*sqlCandidate, error) {
	var primaryName string
	const q = `SELECT node_name FROM shard_primaries WHERE shard_name = $1 AND demoted = false`
	if err := tx.QueryRowContext(ctx, q, s.shardName).Scan(&primaryName); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("error looking up primary: %w", err)
	}

	var primaryNode *sqlCandidate
	if primaryName != "" {
		primaryNode = s.lookupNodeByName(primaryName)
	}

	return primaryNode, nil
}
