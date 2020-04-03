package nodes

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
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
// list.
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

	defer s.updateMetrics()

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
	candidate, err := s.lookupPrimary()

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

// GetPrimary gets the primary of a shard by checking the state of the
// database and updating the internal state. If no primary exists, it
// will be nil. If a primary has been elected but is down, err will be
// ErrPrimaryNotHealthy.
func (s *sqlElector) GetPrimary() (Node, error) {
	primary, err := s.lookupPrimary()

	if err != nil {
		return nil, err
	}

	// Update the internal state so that calls to GetSecondaries() will be
	// consistent with GetPrimary()
	s.setPrimary(primary)

	if primary == nil {
		return nil, ErrPrimaryNotHealthy
	}

	return primary, nil
}

// GetSecondaries gets the secondaries of a shard. It uses the internal
// state to determine the primary so that calls to GetSecondaries() will
// be consistent with the first call to GetPrimary().
func (s *sqlElector) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	primaryNode := s.primaryNode
	s.m.RUnlock()

	var secondaries []Node
	for _, n := range s.nodes {
		if primaryNode != n {
			secondaries = append(secondaries, n)
		}
	}

	return secondaries, nil
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

func (s *sqlElector) getQuorumCount() (int, error) {
	// This is crude form of service discovery. Find how many active
	// Praefect nodes based on whether they attempted to update entries.
	q := `SELECT COUNT (DISTINCT praefect_name) FROM node_status WHERE shard_name = $1 AND last_contact_attempt_at >= NOW() - $2::INTERVAL SECOND`

	var totalCount int

	if err := s.db.QueryRow(q, s.shardName, s.activePraefectSeconds).Scan(&totalCount); err != nil {
		return 0, fmt.Errorf("error retrieving quorum count: %v", err)
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

	q := "DELETE FROM shard_primaries WHERE shard_name = $1"
	_, err := s.db.Exec(q, s.shardName)

	return err
}

func (s *sqlElector) electNewPrimary(candidates []*sqlCandidate) error {
	// Arbitrarily pick the first candidate from the list. Note that the
	// candidate list has already been ranked in order of highest number
	// of Praefect nodes that have reached it and the name of the
	// Praefect identifier.
	newPrimary := candidates[0]

	q := `INSERT INTO shard_primaries (elected_by_praefect, shard_name, node_name, elected_at)
	SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR, NOW()
	WHERE $3 != COALESCE((SELECT node_name FROM shard_primaries WHERE shard_name = $2::VARCHAR), '')
	ON CONFLICT (shard_name)
	DO UPDATE SET elected_by_praefect = EXCLUDED.elected_by_praefect
				, node_name = EXCLUDED.node_name
				, elected_at = EXCLUDED.elected_at
	`
	_, err := s.db.Exec(q, s.praefectName, s.shardName, newPrimary.GetStorage())

	if err != nil {
		s.log.Errorf("error updating new primary: %s", err)
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
		return fmt.Errorf("error retrieving candidates: %v", err)
	}
	defer rows.Close()

	var candidates []*sqlCandidate

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("error retrieving candidate rows: %v", err)
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
	primaryNode, err := s.lookupPrimary()

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

func (s *sqlElector) lookupPrimary() (*sqlCandidate, error) {
	var primaryName string

	q := `SELECT node_name FROM shard_primaries WHERE shard_name = $1`

	if err := s.db.QueryRow(q, s.shardName).Scan(&primaryName); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("error looking up primary: %v", err)
	}

	var primaryNode *sqlCandidate
	if primaryName != "" {
		primaryNode = s.lookupNodeByName(primaryName)
	}

	return primaryNode, nil
}
