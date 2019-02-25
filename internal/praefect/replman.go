package praefect

// ReplicationManager tracks the progress of RPCs being applied to multiple
// downstream servers that make up a shard.
type ReplicationManager struct {
	mu sync.Mutex
	map[string]*Shard // maps a project to a shard
}

type State struct {
	Checksum []byte
}

type Shard struct {
	primary string // the storage location for the replica
	
	// Replicas maps a storage location to a state
	replicas map[string]State
}

func (s *Shard) isConsistent() bool {
	for _, r := range s.Replicas {
		
	}
}

type MutationTracker struct {
	
}


func (mt *MutationTracker) Abort() {
	
}

func (mt *MutationTracker) Done() {
	
}

func (rm *ReplicationManager) StartMutation(ctx context.Context, project, storage string) {
	
}



func (rm *ReplMan) Access(ctx context.Context)

