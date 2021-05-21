package service

import (
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
)

// Dependencies assembles set of components required by different kinds of services.
type Dependencies struct {
	Cfg                 config.Cfg
	RubyServer          *rubyserver.Server
	GitalyHookManager   gitalyhook.Manager
	TransactionManager  transaction.Manager
	StorageLocator      storage.Locator
	ClientPool          *client.Pool
	GitCmdFactory       git.CommandFactory
	Linguist            *linguist.Instance
	BackchannelRegistry *backchannel.Registry
	GitlabClient        gitlab.Client
	CatfileCache        catfile.Cache
	DiskCache           *cache.Cache
}

// GetCfg returns service configuration.
func (dc *Dependencies) GetCfg() config.Cfg {
	return dc.Cfg
}

// GetRubyServer returns client for the ruby processes.
func (dc *Dependencies) GetRubyServer() *rubyserver.Server {
	return dc.RubyServer
}

// GetHookManager returns hook manager.
func (dc *Dependencies) GetHookManager() gitalyhook.Manager {
	return dc.GitalyHookManager
}

// GetTxManager returns transaction manager.
func (dc *Dependencies) GetTxManager() transaction.Manager {
	return dc.TransactionManager
}

// GetLocator returns storage locator.
func (dc *Dependencies) GetLocator() storage.Locator {
	return dc.StorageLocator
}

// GetConnsPool returns gRPC connection pool.
func (dc *Dependencies) GetConnsPool() *client.Pool {
	return dc.ClientPool
}

// GetGitCmdFactory returns git commands factory.
func (dc *Dependencies) GetGitCmdFactory() git.CommandFactory {
	return dc.GitCmdFactory
}

// GetLinguist returns linguist.
func (dc *Dependencies) GetLinguist() *linguist.Instance {
	return dc.Linguist
}

// GetBackchannelRegistry returns a registry of the backchannels.
func (dc *Dependencies) GetBackchannelRegistry() *backchannel.Registry {
	return dc.BackchannelRegistry
}

// GetGitlabClient returns client to access GitLab API.
func (dc *Dependencies) GetGitlabClient() gitlab.Client {
	return dc.GitlabClient
}

// GetCatfileCache returns catfile cache.
func (dc *Dependencies) GetCatfileCache() catfile.Cache {
	return dc.CatfileCache
}

// GetDiskCache returns the disk cache.
func (dc *Dependencies) GetDiskCache() *cache.Cache {
	return dc.DiskCache
}
