package remote

import (
	"bufio"
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
)

var stdRefmaps = map[string]string{
	"all_refs": "+refs/*:refs/*",
	"heads":    "+refs/heads/*:refs/heads/*",
	"tags":     "+refs/tags/*:refs/tags/*",
}

//Add remote to repository
func Add(ctx context.Context, repo repository.GitRepo, name string, url string, refmaps []string) error {
	hasRemote, err := Exists(ctx, repo, name)
	if err != nil {
		return err
	}

	if hasRemote {
		cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
			Name: "remote",
			Flags: []git.Option{
				git.SubSubCmd{"set-url"},
			},
			Args: []string{name, url},
		})
		if err != nil {
			return err
		}
		return cmd.Wait()
	}

	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name: "remote",
		Args: []string{"add", name, url},
	})
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}

	if len(refmaps) > 0 {
		err = setMirror(ctx, repo, name, refmaps)
		if err != nil {
			return err
		}
	}

	return nil
}

//Remove removes the remote from repository
func Remove(ctx context.Context, repo repository.GitRepo, name string) error {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name:  "remote",
		Flags: []git.Option{git.SubSubCmd{Name: "remove"}},
		Args:  []string{name},
	})
	if err != nil {
		return err
	}

	return cmd.Wait()
}

// Exists will always return a boolean value, but should only be depended on
// when the error value is nil
func Exists(ctx context.Context, repo repository.GitRepo, name string) (bool, error) {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{Name: "remote"})
	if err != nil {
		return false, err
	}

	found := false
	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		if scanner.Text() == name {
			found = true
			break
		}
	}

	return found, cmd.Wait()
}

func setMirror(ctx context.Context, repo repository.GitRepo, name string, refmaps []string) error {
	parsedMaps := parseRefmaps(refmaps)

	if len(parsedMaps) == 0 {
		return nil
	}

	for _, configOption := range []string{"mirror", "prune"} {
		cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
			Name: "config",
			Flags: []git.Option{
				git.Flag{"--add"},
				git.ConfigPair{"remote." + name + "." + configOption, "true"},
			},
		})
		if err != nil {
			return err
		}
		if err := cmd.Wait(); err != nil {
			return err
		}
	}

	return setRefmaps(ctx, repo, name, parsedMaps)
}

func setRefmaps(ctx context.Context, repo repository.GitRepo, name string, refmaps []string) error {
	for i, refmap := range refmaps {
		var flag git.Flag
		if i == 0 {
			flag = git.Flag{"--replace-all"}
		} else {
			flag = git.Flag{"--add"}
		}

		cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
			Name: "config",
			Flags: []git.Option{
				flag,
				git.ConfigPair{"remote." + name + ".fetch", refmap},
			},
		})
		if err != nil {
			return err
		}
		if err := cmd.Wait(); err != nil {
			return err
		}
	}

	return nil
}

func parseRefmaps(refmaps []string) []string {
	var parsedMaps []string

	for _, refmap := range refmaps {
		if len(refmap) == 0 {
			continue
		}

		expanded, ok := stdRefmaps[refmap]
		if !ok {
			expanded = refmap
		}
		parsedMaps = append(parsedMaps, expanded)
	}

	return parsedMaps
}
