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
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
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
	// job is no longer relevant (e.g. a node is moved out of a repository)
	JobStateCancelled
)

// ReplJob is an instance of a queued replication job. A replication job is
// meant for updating the repository so that it is synced with the primary
// copy. Scheduled indicates when a replication job should be performed.
type ReplJob struct {
	ID                     uint64            // autoincrement ID
	TargetNode, SourceNode models.Node       // which node to replicate to?
	Repository             models.Repository // source for replication
	State                  JobState
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
	ReplicasDatastore
}

// ReplicasDatastore manages accessing and setting which secondary replicas
// backup a repository
type ReplicasDatastore interface {
	GetReplicas(relativePath string) ([]models.Node, error)

	GetStorageNode(nodeID int) (models.Node, error)

	GetStorageNodes() ([]models.Node, error)

	GetHealthyStorageNodes() ([]models.Node, error)

	GetPrimary(relativePath string) (*models.Node, error)

	SetPrimary(relativePath string, storageNodeID int) error

	AddReplica(relativePath string, storageNodeID int) error

	RemoveReplica(relativePath string, storageNodeID int) error

	GetRepository(relativePath string) (*models.Repository, error)

	UpdateStorageNode(storageNodeID int, healthy bool) error

	Failover(storageNodeID int) error
}

// ReplJobsDatastore represents the behavior needed for fetching and updating
// replication jobs from the datastore
type ReplJobsDatastore interface {
	// GetJobs fetches a list of chronologically ordered replication
	// jobs for the given storage replica. The returned list will be at most
	// count-length.
	GetJobs(flag JobState, nodeID int, count int) ([]ReplJob, error)

	// CreateReplicaJobs will create replication jobs for each secondary
	// replica of a repository known to the datastore. A set of replication job
	// ID's for the created jobs will be returned upon success.
	CreateReplicaReplJobs(relativePath string) ([]uint64, error)

	// UpdateReplJob updates the state of an existing replication job
	UpdateReplJob(jobID uint64, newState JobState) error
}

type jobRecord struct {
	relativePath               string // project's relative path
	targetNodeID, sourceNodeID int
	state                      JobState
}

// MemoryDatastore is a simple datastore that isn't persisted to disk. It is
// only intended for early beta requirements and as a reference implementation
// for the eventual SQL implementation
type MemoryDatastore struct {
	jobs *struct {
		sync.RWMutex
		records map[uint64]jobRecord // all jobs indexed by ID
	}

	storageNodes *struct {
		sync.RWMutex
		m map[int]models.Node
	}

	repositories *struct {
		sync.RWMutex
		m map[string]models.Repository
	}
}

// NewMemoryDatastore returns an initialized in-memory datastore
func NewMemoryDatastore(cfg config.Config) *MemoryDatastore {
	m := &MemoryDatastore{
		storageNodes: &struct {
			sync.RWMutex
			m map[int]models.Node
		}{
			m: map[int]models.Node{},
		},
		jobs: &struct {
			sync.RWMutex
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			records: map[uint64]jobRecord{},
		},
		repositories: &struct {
			sync.RWMutex
			m map[string]models.Repository
		}{
			m: map[string]models.Repository{},
		},
	}

	for i, storageNode := range cfg.Nodes {
		storageNode.ID = i
		m.storageNodes.m[i] = *storageNode
	}

	return m
}

// GetReplicas gets the secondaries for a repository based on the relative path
func (md *MemoryDatastore) GetReplicas(relativePath string) ([]models.Node, error) {
	md.repositories.RLock()
	md.storageNodes.RLock()
	defer md.storageNodes.RUnlock()
	defer md.repositories.RUnlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return nil, errors.New("repository not found")
	}

	return repository.Replicas, nil
}

// GetStorageNode gets all storage nodes
func (md *MemoryDatastore) GetStorageNode(nodeID int) (models.Node, error) {
	md.storageNodes.RLock()
	defer md.storageNodes.RUnlock()

	node, ok := md.storageNodes.m[nodeID]
	if !ok {
		return models.Node{}, errors.New("node not found")
	}

	return node, nil
}

// GetStorageNodes gets all storage nodes
func (md *MemoryDatastore) GetStorageNodes() ([]models.Node, error) {
	md.storageNodes.RLock()
	defer md.storageNodes.RUnlock()

	var storageNodes []models.Node
	for _, storageNode := range md.storageNodes.m {
		storageNodes = append(storageNodes, storageNode)
	}

	return storageNodes, nil
}

// GetStorageHealthyNodes gets all storage nodes
func (md *MemoryDatastore) GetHealthyStorageNodes() ([]models.Node, error) {
	md.storageNodes.RLock()
	defer md.storageNodes.RUnlock()

	var storageNodes []models.Node
	for _, storageNode := range md.storageNodes.m {
		if storageNode.Healthy {
			storageNodes = append(storageNodes, storageNode)
		}
	}

	return storageNodes, nil
}

// GetPrimary gets the primary storage node for a repository of a repository relative path
func (md *MemoryDatastore) GetPrimary(relativePath string) (*models.Node, error) {
	md.repositories.RLock()
	defer md.repositories.RUnlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return nil, ErrPrimaryNotSet
	}

	storageNode, ok := md.storageNodes.m[repository.Primary.ID]
	if !ok {
		return nil, errors.New("node storage not found")
	}
	return &storageNode, nil

}

// SetPrimary sets the primary storagee node for a repository of a repository relative path
func (md *MemoryDatastore) SetPrimary(relativePath string, storageNodeID int) error {
	md.repositories.Lock()
	defer md.repositories.Unlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		repository = models.Repository{RelativePath: relativePath}
	}

	storageNode, ok := md.storageNodes.m[storageNodeID]
	if !ok {
		return errors.New("node storage not found")
	}

	repository.Primary = storageNode

	md.repositories.m[relativePath] = repository
	return nil
}

// AddReplica adds a secondary to a repository of a repository relative path
func (md *MemoryDatastore) AddReplica(relativePath string, storageNodeID int) error {
	md.repositories.Lock()
	defer md.repositories.Unlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return errors.New("repository not found")
	}

	storageNode, ok := md.storageNodes.m[storageNodeID]
	if !ok {
		return errors.New("node storage not found")
	}

	repository.Replicas = append(repository.Replicas, storageNode)

	md.repositories.m[relativePath] = repository
	return nil
}

// RemoveReplica removes a secondary from a repository of a repository relative path
func (md *MemoryDatastore) RemoveReplica(relativePath string, storageNodeID int) error {
	md.repositories.Lock()
	defer md.repositories.Unlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return errors.New("repository not found")
	}

	var secondaries []models.Node
	for _, secondary := range repository.Replicas {
		if secondary.ID != storageNodeID {
			secondaries = append(secondaries, secondary)
		}
	}

	repository.Replicas = secondaries
	md.repositories.m[relativePath] = repository
	return nil
}

// GetRepository gets the repository for a repository relative path
func (md *MemoryDatastore) GetRepository(relativePath string) (*models.Repository, error) {
	md.repositories.Lock()
	defer md.repositories.Unlock()

	repository, ok := md.repositories.m[relativePath]
	if !ok {
		return nil, errors.New("repository not found")
	}

	return &repository, nil
}

// ErrReplicasMissing indicates the repository does not have any backup
// replicas
var ErrReplicasMissing = errors.New("repository missing secondary replicas")

// GetJobs is a more general method to retrieve jobs of a certain state from the datastore
func (md *MemoryDatastore) GetJobs(state JobState, targetNodeID int, count int) ([]ReplJob, error) {
	md.jobs.RLock()
	defer md.jobs.RUnlock()

	var results []ReplJob

	for i, record := range md.jobs.records {
		// state is a bitmap that is a combination of one or more JobStates
		if record.state&state != 0 && record.targetNodeID == targetNodeID {
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
// referencing the current repository for the project being replicated
func (md *MemoryDatastore) replJobFromRecord(jobID uint64, record jobRecord) (ReplJob, error) {
	repository, err := md.GetRepository(record.relativePath)
	if err != nil {
		return ReplJob{}, err
	}

	sourceNode, err := md.GetStorageNode(record.sourceNodeID)
	if err != nil {
		return ReplJob{}, err
	}
	targetNode, err := md.GetStorageNode(record.targetNodeID)
	if err != nil {
		return ReplJob{}, err
	}

	return ReplJob{
		ID:         jobID,
		Repository: *repository,
		SourceNode: sourceNode,
		State:      record.state,
		TargetNode: targetNode,
	}, nil
}

// ErrInvalidReplTarget indicates a targetStorage repository cannot be chosen because
// it fails preconditions for being replicatable
var ErrInvalidReplTarget = errors.New("targetStorage repository fails preconditions for replication")

// CreateReplicaReplJobs creates a replication job for each secondary that
// backs the specified repository. Upon success, the job IDs will be returned.
func (md *MemoryDatastore) CreateReplicaReplJobs(relativePath string) ([]uint64, error) {
	md.jobs.Lock()
	defer md.jobs.Unlock()

	if relativePath == "" {
		return nil, errors.New("invalid source repository")
	}

	repository, err := md.GetRepository(relativePath)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to find repository for project at relative path %q",
			relativePath,
		)
	}

	var jobIDs []uint64

	for _, secondary := range repository.Replicas {
		nextID := uint64(len(md.jobs.records) + 1)

		md.jobs.records[nextID] = jobRecord{
			targetNodeID: secondary.ID,
			state:        JobStatePending,
			relativePath: relativePath,
			sourceNodeID: repository.Primary.ID,
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

// Failover replaces any repository with storage as its primary with one of its replicas
func (md *MemoryDatastore) Failover(storageNodeID int) error {
	md.repositories.Lock()
	defer md.repositories.Unlock()

	md.UpdateStorageNode(storageNodeID, true)

	for relativePath, repository := range md.repositories.m {
		if repository.Primary.ID == storageNodeID {
			if len(repository.Replicas) > 0 {
				repository.Primary = repository.Replicas[0]
				repository.Replicas = repository.Replicas[1:]
				md.repositories.m[relativePath] = repository
			}
		}
	}
	return nil
}

func (md *MemoryDatastore) UpdateStorageNode(nodeID int, healthy bool) error {
	md.storageNodes.Lock()
	defer md.storageNodes.Unlock()

	storageNode, ok := md.storageNodes.m[nodeID]
	if !ok {
		return errors.New("node not found")
	}
	storageNode.Healthy = healthy
	md.storageNodes.m[nodeID] = storageNode

	return nil
}
