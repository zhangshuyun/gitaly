package stats

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
)

// HasBitmap returns whether or not the repository contains an object bitmap.
func HasBitmap(repoPath string) (bool, error) {
	hasBitmap, err := hasBitmap(repoPath)
	if err != nil {
		return false, err
	}
	return hasBitmap, nil
}

// PackfilesCount returns the number of packfiles a repository has.
func PackfilesCount(repoPath string) (int, error) {
	packFiles, err := GetPackfiles(repoPath)
	if err != nil {
		return 0, err
	}

	return len(packFiles), nil
}

// GetPackfiles returns the FileInfo of packfiles inside a repository.
func GetPackfiles(repoPath string) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(filepath.Join(repoPath, "objects/pack/"))
	if err != nil {
		return nil, err
	}

	var packFiles []os.FileInfo
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".pack" {
			packFiles = append(packFiles, f)
		}
	}

	return packFiles, nil
}

// UnpackedObjects returns the number of loose objects that have a timestamp later than the newest
// packfile.
func UnpackedObjects(repoPath string) (int64, error) {
	unpackedObjects, err := getUnpackedObjects(repoPath)
	if err != nil {
		return 0, err
	}

	return unpackedObjects, nil
}

// LooseObjects returns the number of loose objects that are not in a packfile.
func LooseObjects(ctx context.Context, gitCmdFactory git.CommandFactory, repository repository.GitRepo) (int64, error) {
	cmd, err := gitCmdFactory.New(ctx, repository, git.SubCmd{Name: "count-objects", Flags: []git.Option{git.Flag{Name: "--verbose"}}})
	if err != nil {
		return 0, err
	}

	objectStats, err := readObjectInfoStatistic(cmd)
	if err != nil {
		return 0, err
	}

	count, ok := objectStats["count"].(int64)
	if !ok {
		return 0, errors.New("could not get object count")
	}

	return count, nil
}

func hasBitmap(repoPath string) (bool, error) {
	bitmaps, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.bitmap"))
	if err != nil {
		return false, err
	}

	return len(bitmaps) > 0, nil
}

func getUnpackedObjects(repoPath string) (int64, error) {
	objectDir := filepath.Join(repoPath, "objects")

	packFiles, err := filepath.Glob(filepath.Join(objectDir, "pack", "*.pack"))
	if err != nil {
		return 0, err
	}

	var newestPackfileModtime time.Time

	for _, packFilePath := range packFiles {
		stat, err := os.Stat(packFilePath)
		if err != nil {
			return 0, err
		}
		if stat.ModTime().After(newestPackfileModtime) {
			newestPackfileModtime = stat.ModTime()
		}
	}

	var unpackedObjects int64
	if err = filepath.Walk(objectDir, func(path string, info os.FileInfo, err error) error {
		if objectDir == path {
			return nil
		}

		if info.IsDir() {
			if err := skipNonObjectDir(objectDir, path); err != nil {
				return err
			}
		}

		if !info.IsDir() && info.ModTime().After(newestPackfileModtime) {
			unpackedObjects++
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return unpackedObjects, nil
}

func skipNonObjectDir(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}

	if len(rel) != 2 {
		return filepath.SkipDir
	}

	if _, err := strconv.ParseUint(rel, 16, 8); err != nil {
		return filepath.SkipDir
	}

	return nil
}
