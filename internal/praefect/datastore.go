// Package praefect provides data models and datastore persistence abstractions
// for tracking the state of repository replicas.
//
// See original design discussion:
// https://gitlab.com/gitlab-org/gitaly/issues/1495
package praefect

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

var (
	// ErrPrimaryNotSet indicates the primary has not been set in the datastore
	ErrPrimaryNotSet = errors.New("primary is not set")
)

// JobState is an enum that indicates the state of a job
type JobState uint8

const (
	// JobStatePending is the initial job state when it is not yet ready to run
	// and may indicate recovery from a failure prior to the ready-state
	JobStatePending JobState = 1 << iota
	// JobStateReady indicates the job is now ready to proceed
	JobStateReady
	// JobStateInProgress indicates the job is being processed by a worker
	JobStateInProgress
	// JobStateComplete indicates the job is now complete
	JobStateComplete
	// JobStateCancelled indicates the job was cancelled. This can occur if the
	// job is no longer relevant (e.g. a node is moved out of a shard)
	JobStateCancelled
)

// ReplJob is an instance of a queued replication job. A replication job is
// meant for updating the repository so that it is synced with the primary
// copy. Scheduled indicates when a replication job should be performed.
type ReplJob struct {
	ID     uint64     // autoincrement ID
	Target string     // which storage location to replicate to?
	Source Repository // source for replication
	State  JobState
}

// replJobs provides sort manipulation behavior
type replJobs []ReplJob

func (rjs replJobs) Len() int      { return len(rjs) }
func (rjs replJobs) Swap(i, j int) { rjs[i], rjs[j] = rjs[j], rjs[i] }

// byJobID provides a comparator for sorting jobs
type byJobID struct{ replJobs }

func (b byJobID) Less(i, j int) bool { return b.replJobs[i].ID < b.replJobs[j].ID }

// Datastore is a data persistence abstraction for all of Praefect's
// persistence needs
type Datastore interface {
	ReplJobsDatastore
	NodesDatastore
}

// NodesDatastore manages accessing and setting the primary storage location
type NodesDatastore interface {
	GetDefaultPrimary() (Node, error)

	GetPrimary(repo Repository) (Node, error)

	SetPrimary(repo Repository, node Node) error

	GetSecondaries(repo Repository) ([]Node, error)

	RemoveSecondary(repo Repository, node Node) error

	AddSecondary(repo Repository, node Node) error
}

// ReplJobsDatastore represents the behavior needed for fetching and updating
// replication jobs from the datastore
type ReplJobsDatastore interface {
	// GetJobs fetches a list of chronologically ordered replication
	// jobs for the given storage replica. The returned list will be at most
	// count-length.
	GetJobs(flag JobState, storage string, count int) ([]ReplJob, error)

	// CreateSecondaryJobs will create replication jobs for each secondary
	// replica of a repository known to the datastore. A set of replication job
	// ID's for the created jobs will be returned upon success.
	CreateSecondaryReplJobs(source Repository) ([]uint64, error)

	// UpdateReplJob updates the state of an existing replication job
	UpdateReplJob(jobID uint64, newState JobState) error
}

// shard is a set of primary and secondary storage replicas for a project
type shard struct {
	primary     string
	secondaries []string
}

type jobRecord struct {
	relativePath string // project's relative path
	target       string
	state        JobState
}

// MemoryDatastore is a simple datastore that isn't persisted to disk. It is
// only intended for early beta requirements and as a reference implementation
// for the eventual SQL implementation
type MemoryDatastore struct {
	jobs *struct {
		sync.RWMutex
		next    uint64
		records map[uint64]jobRecord // all jobs indexed by ID
	}

	nodes *struct {
		sync.RWMutex
		reposToSecondaries map[string][]Node
		reposToPrimary     map[string]Node
		defaultPrimary     Node
	}
}

// NewMemoryDatastore returns an initialized in-memory datastore
func NewMemoryDatastore(cfg config.Config) *MemoryDatastore {
	m := &MemoryDatastore{
		jobs: &struct {
			sync.RWMutex
			next    uint64
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			next:    0,
			records: map[uint64]jobRecord{},
		},
		nodes: &struct {
			sync.RWMutex
			reposToSecondaries map[string][]Node
			reposToPrimary     map[string]Node
			defaultPrimary     Node
		}{
			reposToSecondaries: make(map[string][]Node),
			reposToPrimary:     make(map[string]Node),
			defaultPrimary: Node{
				Storage: cfg.Servers[0].Name,
				Address: cfg.Servers[0].ListenAddr,
				Token:   "",
			},
		},
	}

	for _, relativePath := range cfg.Whitelist {
		for _, server := range cfg.Servers {
			defaultPrimary, err := m.GetDefaultPrimary()
			if err != nil {
				panic(fmt.Sprintf("could not get default primary: %v", err))
			}
			if server.Name == defaultPrimary.Storage {
				continue
			}
			secondaries, ok := m.nodes.reposToSecondaries[relativePath]
			if !ok {
				secondaries = make([]Node, 0)
			}
			secondaries = append(secondaries, Node{
				Storage: server.Name,
				Address: server.ListenAddr,
				Token:   "",
			})
			m.nodes.reposToSecondaries[relativePath] = secondaries
		}

		// initialize replication job queue to replicate all whitelisted repos
		// to every secondary server
		for _, secondary := range m.nodes.reposToSecondaries[relativePath] {
			m.jobs.next++
			m.jobs.records[m.jobs.next] = jobRecord{
				state:        JobStateReady,
				target:       secondary.Storage,
				relativePath: relativePath,
			}
		}

	}

	return m
}

func (md *MemoryDatastore) GetDefaultPrimary() (Node, error) {
	return md.nodes.defaultPrimary, nil
}

// GetPrimary gets the primary datastore location
func (md *MemoryDatastore) GetPrimary(repo Repository) (Node, error) {
	md.nodes.RLock()
	defer md.nodes.RUnlock()

	primary, ok := md.nodes.reposToPrimary[repo.RelativePath]
	if !ok {
		return md.GetDefaultPrimary()
	}

	return primary, nil
}

func (md *MemoryDatastore) SetPrimary(repo Repository, node Node) error {
	md.nodes.Lock()
	defer md.nodes.Unlock()

	md.nodes.reposToPrimary[repo.RelativePath] = node
	return nil
}

// GetSecondaries will return the set of secondary storage locations for a
// given repository if they exist
func (md *MemoryDatastore) GetSecondaries(repo Repository) ([]Node, error) {
	return md.nodes.reposToSecondaries[repo.RelativePath], nil
}

// RemoveSecondary will return the set of secondary storage locations for a
// given repository if they exist
func (md *MemoryDatastore) RemoveSecondary(repo Repository, node Node) error {

	secondaries, ok := md.nodes.reposToSecondaries[repo.RelativePath]
	if !ok {
		return nil
	}

	for i, secondary := range secondaries {
		if secondary == node {
			secondaries = append(secondaries[:i], secondaries[i+1:]...)
			md.nodes.reposToSecondaries[repo.RelativePath] = secondaries
			return nil
		}
	}

	return nil
}

// AddSecondary will add a secondary to the list of secondaries for a repository
func (md *MemoryDatastore) AddSecondary(repo Repository, node Node) error {
	md.nodes.Lock()
	defer md.nodes.Unlock()

	secondaries, ok := md.nodes.reposToSecondaries[repo.RelativePath]
	if !ok {
		secondaries = make([]Node, 0)
	}
	secondaries = append(secondaries, node)

	md.nodes.reposToSecondaries[repo.RelativePath] = secondaries

	return nil
}

// ErrSecondariesMissing indicates the repository does not have any backup
// replicas
var ErrSecondariesMissing = errors.New("repository missing secondary replicas")

// GetJobs is a more general method to retrieve jobs of a certain state from the datastore
func (md *MemoryDatastore) GetJobs(state JobState, storage string, count int) ([]ReplJob, error) {
	md.jobs.RLock()
	defer md.jobs.RUnlock()

	var results []ReplJob

	for i, record := range md.jobs.records {
		// state is a bitmap that is a combination of one or more JobStates
		if record.state&state != 0 && record.target == storage {
			job, err := md.replJobFromRecord(i, record)
			if err != nil {
				return nil, err
			}

			results = append(results, job)
			if len(results) >= count {
				break
			}
		}
	}

	sort.Sort(byJobID{results})

	return results, nil
}

// replJobFromRecord constructs a replication job from a record and by cross
// referencing the current shard for the project being replicated
func (md *MemoryDatastore) replJobFromRecord(jobID uint64, record jobRecord) (ReplJob, error) {
	primary, err := md.GetPrimary(Repository{RelativePath: record.relativePath})
	if err != nil {
		return ReplJob{}, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			record.relativePath,
		)
	}

	return ReplJob{
		ID: jobID,
		Source: Repository{
			RelativePath: record.relativePath,
			Storage:      primary.Storage,
		},
		State:  record.state,
		Target: record.target,
	}, nil
}

// ErrInvalidReplTarget indicates a target repository cannot be chosen because
// it fails preconditions for being replicatable
var ErrInvalidReplTarget = errors.New("target repository fails preconditions for replication")

// CreateSecondaryReplJobs creates a replication job for each secondary that
// backs the specified repository. Upon success, the job IDs will be returned.
func (md *MemoryDatastore) CreateSecondaryReplJobs(source Repository) ([]uint64, error) {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	emptyRepo := Repository{}
	if source == emptyRepo {
		return nil, errors.New("invalid source repository")
	}

	secondaries, err := md.GetSecondaries(Repository{RelativePath: source.RelativePath})
	if err != nil {
		return nil, fmt.Errorf(
			"unable to find shard for project at relative path %q",
			source.RelativePath,
		)
	}

	var jobIDs []uint64

	for _, secondary := range secondaries {
		nextID := uint64(len(md.jobs.records) + 1)

		md.jobs.next++
		md.jobs.records[md.jobs.next] = jobRecord{
			target:       secondary.Storage,
			state:        JobStatePending,
			relativePath: source.RelativePath,
		}

		jobIDs = append(jobIDs, nextID)
	}

	return jobIDs, nil
}

// UpdateReplJob updates an existing replication job's state
func (md *MemoryDatastore) UpdateReplJob(jobID uint64, newState JobState) error {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	job, ok := md.jobs.records[jobID]
	if !ok {
		return fmt.Errorf("job ID %d does not exist", jobID)
	}

	if newState == JobStateComplete || newState == JobStateCancelled {
		// remove the job to avoid filling up memory with unneeded job records
		delete(md.jobs.records, jobID)
		return nil
	}

	job.state = newState
	md.jobs.records[jobID] = job
	return nil
}
