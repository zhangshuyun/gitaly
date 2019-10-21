package featureflag

const (
	// AddRemoteGo will cause the AddRemote RPC to use the go implementation when set
	AddRemoteGo = "add_remote_go"

	// GetAllLFSPointersGo will cause the GetAllLFSPointers RPC to use the go implementation when set
	GetAllLFSPointersGo = "get_all_lfs_pointers_go"

	// LinguistFileCountStats will invoke an additional git-linguist command to get the number of files per language
	LinguistFileCountStats = "linguist_file_count_stats"

	// UploadPackFilter enables partial clones by sending uploadpack.allowFilter and uploadpack.allowAnySHA1InWant
	// to upload-pack
	UploadPackFilter = "upload_pack_filter"
)
