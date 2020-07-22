package featureflag

type FeatureFlag interface {
	GetName() string
	IsOnByDefault() bool
}

type RubyFeatureFlag struct {
	GoFeatureFlag
}

type GoFeatureFlag struct {
	Name        string `json:"Name"`
	OnByDefault bool   `json:"on_by_default"`
}

func (g GoFeatureFlag) GetName() string {
	return g.Name
}

func (g GoFeatureFlag) IsOnByDefault() bool {
	return g.OnByDefault
}

func NewGoFeatureFlag(Name string, OnByDefault bool) *GoFeatureFlag {
	return &GoFeatureFlag{
		Name:        Name,
		OnByDefault: OnByDefault,
	}
}

var (
	// GoUpdateHook will bypass the ruby update hook and use the go implementation of custom hooks
	GoUpdateHook = GoFeatureFlag{Name: "go_update_hook", OnByDefault: true}
	// RemoteBranchesLsRemote will use `ls-remote` for remote branches
	RemoteBranchesLsRemote = GoFeatureFlag{Name: "ruby_remote_branches_ls_remote", OnByDefault: true}
	// GoFetchSourceBranch enables a go implementation of FetchSourceBranch
	GoFetchSourceBranch = GoFeatureFlag{Name: "go_fetch_source_branch", OnByDefault: false}
	// DistributedReads allows praefect to redirect accessor operations to up-to-date secondaries
	DistributedReads = GoFeatureFlag{Name: "distributed_reads", OnByDefault: false}
	// GoPreReceiveHook will bypass the ruby pre-receive hook and use the go implementation
	GoPreReceiveHook = GoFeatureFlag{Name: "go_prereceive_hook", OnByDefault: true}
	// GoPostReceiveHook will bypass the ruby post-receive hook and use the go implementation
	GoPostReceiveHook = GoFeatureFlag{Name: "go_postreceive_hook", OnByDefault: false}
	// ReferenceTransactions will handle Git reference updates via the transaction service for strong consistency
	ReferenceTransactions = GoFeatureFlag{Name: "reference_transactions", OnByDefault: false}
	// ReferenceTransactionsOperationService will enable reference transactions for the OperationService
	ReferenceTransactionsOperationService = GoFeatureFlag{Name: "reference_transactions_operation_service", OnByDefault: true}
	// ReferenceTransactionsSmartHTTPService will enable reference transactions for the SmartHTTPService
	ReferenceTransactionsSmartHTTPService = GoFeatureFlag{Name: "reference_transactions_smarthttp_service", OnByDefault: true}
	// ReferenceTransactionsSSHService will enable reference transactions for the SSHService
	ReferenceTransactionsSSHService = GoFeatureFlag{Name: "reference_transactions_ssh_service", OnByDefault: true}
)

const (
	GoUpdateHookEnvVar      = "GITALY_GO_UPDATE"
	GoPreReceiveHookEnvVar  = "GITALY_GO_PRERECEIVE"
	GoPostReceiveHookEnvVar = "GITALY_GO_POSTRECEIVE"
)
