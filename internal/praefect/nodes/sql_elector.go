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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

const (
	defaultFailoverTimeoutSeconds = 10
	defaultActivePraefectSeconds  = 60
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
// Every Praefect node periodically (every second) performs a health check RPC with a Gitaly node.
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
	m                     sync.RWMutex
	praefectName          string
	shardName             string
	nodes                 []*sqlCandidate
	primaryNode           *sqlCandidate
	db                    *sql.DB
	log                   logrus.FieldLogger
	failoverSeconds       int
	activePraefectSeconds int
	readOnlyAfterFailover bool

	getShardTotal prometheus.Counter
	neverSet      prometheus.Gauge
}

func newSQLElector(name string, c config.Config, failoverTimeoutSeconds int, activePraefectSeconds int, db *sql.DB, log logrus.FieldLogger, ns []*nodeStatus) *sqlElector {
	praefectName := getPraefectName(c, log)

	log = log.WithField("praefectName", praefectName)
	log.Info("Using SQL election strategy")

	nodes := make([]*sqlCandidate, len(ns))
	for i, n := range ns {
		nodes[i] = &sqlCandidate{Node: n}
	}

	return &sqlElector{
		praefectName:          praefectName,
		shardName:             name,
		db:                    db,
		log:                   log,
		failoverSeconds:       failoverTimeoutSeconds,
		activePraefectSeconds: activePraefectSeconds,
		nodes:                 nodes,
		primaryNode:           nodes[0],
		readOnlyAfterFailover: c.Failover.ReadOnlyAfterFailover,

		// We create the counter here with all the label values already set. This means the time series
		// shows up immediately after registration in Prometheus without any prior observations being made.
		// The metric is defined where it is used, reducing the need to jump around the code base.
		getShardTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gitaly_get_shard_total",
			Help: "Total count of GetShard calls",
			ConstLabels: prometheus.Labels{
				"virtual_storage": name,
			},
		}),

		neverSet: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gitaly_never_set_gauge",
			Help: "This is never touched, yet is visible in scrape",
			ConstLabels: prometheus.Labels{
				"virtual_storage": name,
			},
		}),
	}
}

// Generate a Praefect name so that each Praefect process can report
// node statuses independently.  This will enable us to do a SQL
// election to determine which nodes are active. Ideally this name
// doesn't change across restarts since that may temporarily make it
// look like there are more Praefect processes active for
// determining a quorum.
func getPraefectName(c config.Config, log logrus.FieldLogger) string {
	name, err := os.Hostname()

	if err != nil {
		name = uuid.New().String()
		log.WithError(err).WithFields(logrus.Fields{
			"praefectName": name,
		}).Warn("unable to determine Praefect hostname, using randomly generated UUID")
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
	var wg sync.WaitGroup

	for _, n := range s.nodes {
		wg.Add(1)

		go func(n Node) {
			defer wg.Done()
			result, _ := n.check(ctx)
			if err := s.updateNode(n, result); err != nil {
				s.log.WithError(err).WithFields(logrus.Fields{
					"shard":   s.shardName,
					"storage": n.GetStorage(),
					"address": n.GetAddress(),
				}).Error("error checking node")
			}
		}(n)
	}

	wg.Wait()

	err := s.validateAndUpdatePrimary()

	if err != nil {
		s.log.WithError(err).Error("unable to validate primary")
		return err
	}

	// The attempt to elect a primary may have conflicted with another
	// node attempting to elect a primary. We check the database again
	// to see the current state.
	candidate, _, err := s.lookupPrimary()
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
			"shard":      s.shardName}).Info("primary node changed")

		s.primaryNode = candidate
	}
}

func (s *sqlElector) updateNode(node Node, result bool) error {
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

	_, err := s.db.Exec(q, s.praefectName, s.shardName, node.GetStorage())

	if err != nil {
		s.log.Errorf("Error updating node: %s", err)
	}

	return err
}

// GetShard gets the current status of the shard. ErrPrimaryNotHealthy
// is returned if a primary does not exist.
func (s *sqlElector) GetShard() (Shard, error) {
	// Since we set the constant labels when we created the counter,
	// we can simply increment here without worrying about the labels.
	// Since the metric is always created, we do not need to nil check
	// either.
	s.getShardTotal.Inc()

	primary, readOnly, err := s.lookupPrimary()
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
		IsReadOnly:  readOnly,
		Primary:     primary,
		Secondaries: secondaries,
	}, nil
}

func (s *sqlElector) Describe(descs chan<- *prometheus.Desc) {
	// DescribeByCollect is a convenience method from prometheus that allows us to avoid
	// explicitly listing every metric here and only focus on the Collect method.
	prometheus.DescribeByCollect(s, descs)
}

func (s *sqlElector) Collect(metrics chan<- prometheus.Metric) {
	// First we collect every concrete metric we have
	s.getShardTotal.Collect(metrics)
	s.neverSet.Collect(metrics)

	// Then we collect metrics that do not directly map to a persistent
	// metric. In this case, the primary information comes from the database
	// and could have been modified by another praefect node. We get the value
	// when it is needed for a scrape, guaranteeing it is always up to date.
	// This is also useful if we have other derived metrics that we do not hold
	// in the memory.
	shard, err := s.GetShard()
	if err != nil {
		// In the example, we only log the error. We could also expose a metric
		// indicating the scrape's failure: https://prometheus.io/docs/instrumenting/writing_exporters/#failed-scrapes
		s.log.WithFields(logrus.Fields{
			"error":           err,
			"virtual-storage": s.shardName,
		}).Error("prometheus scrape failed")
		return
	}

	for _, node := range s.nodes {
		var val float64
		if node == shard.Primary {
			val = 1
		}

		// Since the information about the primary is derived from GetShard during scrape, we do not have a persistent
		// gauge for it. Instead, we just give the derived value directly to Prometheus via a ConstMetric. Since there
		// are no gauges that could be set from the outside during a scrape, the view is always consistent.
		metrics <- prometheus.MustNewConstMetric(prometheus.NewDesc(
			"gitaly_praefect_primaries",
			"Primary/Secondary statuses of Gitaly nodes.",
			[]string{"virtual_storage", "gitaly_storage"},
			nil,
		), prometheus.GaugeValue, val, s.shardName, node.GetStorage())
	}
}

func (s *sqlElector) getQuorumCount() (int, error) {
	// This is crude form of service discovery. Find how many active
	// Praefect nodes based on whether they attempted to update entries.
	q := `SELECT COUNT (DISTINCT praefect_name) FROM node_status WHERE shard_name = $1 AND last_contact_attempt_at >= NOW() - $2::INTERVAL SECOND`

	var totalCount int

	if err := s.db.QueryRow(q, s.shardName, s.activePraefectSeconds).Scan(&totalCount); err != nil {
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

func (s *sqlElector) demotePrimary() error {
	s.setPrimary(nil)

	q := "UPDATE shard_primaries SET demoted = true WHERE shard_name = $1"
	_, err := s.db.Exec(q, s.shardName)

	return err
}

// targetNodeIncompleteCounts represents a row of the sql election query and is for logging purposes only
type targetNodeIncompleteCounts struct {
	NodeStorage string `json:"node_storage"`
	Ready       int    `json:"ready"`
	InProgress  int    `json:"in_progress"`
	Failed      int    `json:"failed"`
	Dead        int    `json:"dead"`
}

func (s *sqlElector) electNewPrimary(candidates []*sqlCandidate) error {
	if len(candidates) == 0 {
		return errors.New("candidates cannot be empty")
	}

	candidateStorages := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		candidateStorages = append(candidateStorages, candidate.GetStorage())
	}

	q := `  SELECT target_node_storage, SUM(ready) AS ready, SUM(in_progress) AS in_progress, SUM(failed) AS failed, SUM(dead) AS dead
	        FROM (
	            SELECT
	                CASE WHEN rq.state = 'ready' THEN 1 ELSE 0 END AS ready,
	                CASE WHEN rq.state = 'in_progress' THEN 1 ELSE 0 END AS in_progress,
	                CASE WHEN rq.state = 'failed' THEN 1 ELSE 0 END AS failed,
	                CASE WHEN rq.state = 'dead' THEN 1 ELSE 0 END AS dead,
	                rq.job->>'target_node_storage' AS target_node_storage
	            FROM replication_queue AS rq
	            JOIN (
	            	SELECT
	            		job->>'target_node_storage' AS target_node_storage,
	            		MAX(updated_at) AS updated_at
	            	FROM replication_queue
	            	WHERE state = 'completed' AND job->>'target_node_storage' = ANY ($1) AND job->>'virtual_storage' = $2
	            	GROUP BY job->>'target_node_storage'
	            ) latest ON rq.job->>'target_node_storage' = latest.target_node_storage AND rq.updated_at >= latest.updated_at
	            WHERE rq.job->>'virtual_storage' = $2
	        ) AS t
	        GROUP BY target_node_storage
	        ORDER BY SUM(ready+in_progress+2*failed+2*dead)`

	rows, err := s.db.Query(q, pq.Array(candidateStorages), s.shardName)
	if err != nil {
		return fmt.Errorf("executing query for ordering candidate nodes: %w", err)
	}
	defer rows.Close()

	var incompleteCounts []targetNodeIncompleteCounts

	newPrimaryStorage := candidateStorages[0]
	for rows.Next() {
		var r targetNodeIncompleteCounts
		if err := rows.Scan(&r.NodeStorage, &r.Ready, &r.InProgress, &r.Failed, &r.Dead); err != nil {
			return fmt.Errorf("scanning rows for incomplete count: %w", err)
		}

		incompleteCounts = append(incompleteCounts, r)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("getting rows for ordering candidate nodes: %w", err)
	}

	if len(incompleteCounts) > 0 {
		newPrimaryStorage = incompleteCounts[0].NodeStorage
		s.log.WithField("incomplete_counts", incompleteCounts).WithField("new_primary", newPrimaryStorage).Info("new primary selected")
	}

	// read_only is set only when a row already exists in the table. This avoids new shards, which
	// do not yet have a row in the table, from starting in read-only mode. In a failover scenario,
	// a row already exists in the table denoting the previous primary, and thus the shard should
	// be switched to read-only mode.
	q = `INSERT INTO shard_primaries (elected_by_praefect, shard_name, node_name, elected_at)
	SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR, NOW()
	WHERE $3 != COALESCE((SELECT node_name FROM shard_primaries WHERE shard_name = $2::VARCHAR AND demoted = false), '')
	ON CONFLICT (shard_name)
	DO UPDATE SET elected_by_praefect = EXCLUDED.elected_by_praefect
				, node_name = EXCLUDED.node_name
				, elected_at = EXCLUDED.elected_at
				, read_only = $5
				, demoted = false
	   WHERE shard_primaries.elected_at < now() - $4::INTERVAL SECOND
	`
	_, err = s.db.Exec(q, s.praefectName, s.shardName, newPrimaryStorage, s.failoverSeconds, s.readOnlyAfterFailover)

	if err != nil {
		s.log.Errorf("error updating new primary: %s", err)
		return err
	}

	return nil
}

func (s *sqlElector) enableWrites(ctx context.Context) error {
	const q = "UPDATE shard_primaries SET read_only = false WHERE shard_name = $1 AND demoted = false"
	if rslt, err := s.db.ExecContext(ctx, q, s.shardName); err != nil {
		return err
	} else if n, err := rslt.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrPrimaryNotHealthy
	}

	return nil
}

func (s *sqlElector) validateAndUpdatePrimary() error {
	quorumCount, err := s.getQuorumCount()

	if err != nil {
		return err
	}

	// Retrieves candidates, ranked by the ones that are the most active
	q := `SELECT node_name FROM node_status
			WHERE shard_name = $1 AND last_seen_active_at >= NOW() - $2::INTERVAL SECOND
			GROUP BY node_name
			HAVING COUNT(praefect_name) >= $3
			ORDER BY COUNT(node_name) DESC, node_name ASC`

	rows, err := s.db.Query(q, s.shardName, s.failoverSeconds, quorumCount)

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
	primaryNode, _, err := s.lookupPrimary()

	if err != nil {
		s.log.WithError(err).Error("error looking up primary")
		return err
	}

	if len(candidates) == 0 {
		return s.demotePrimary()
	}

	if primaryNode == nil || !nodeInSlice(candidates, primaryNode) {
		return s.electNewPrimary(candidates)
	}

	return nil
}

func (s *sqlElector) lookupPrimary() (*sqlCandidate, bool, error) {
	var primaryName string
	var readOnly bool

	const q = `SELECT node_name, read_only FROM shard_primaries WHERE shard_name = $1 AND demoted = false`
	if err := s.db.QueryRow(q, s.shardName).Scan(&primaryName, &readOnly); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("error looking up primary: %w", err)
	}

	var primaryNode *sqlCandidate
	if primaryName != "" {
		primaryNode = s.lookupNodeByName(primaryName)
	}

	return primaryNode, readOnly, nil
}
