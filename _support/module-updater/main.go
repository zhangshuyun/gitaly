package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var skipDirs = map[string]bool{
	".git":                              true,
	".gitlab":                           true,
	"_build":                            true,
	"_support":                          true,
	"changelogs":                        true,
	"danger":                            true,
	"doc":                               true,
	"proto/go/gitalypb":                 true,
	"proto/go/internal/linter/testdata": true,
	"ruby":                              true,
	"scripts":                           true,
	"unreleased":                        true,
}

func main() {
	if err := changeModuleVersion(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func changeModuleVersion() error {
	dirLocation, fromVersion, toVersion, err := getInput()
	if err != nil {
		return err
	}

	moduleAbsRootPath, err := filepath.Abs(dirLocation)
	if err != nil {
		return fmt.Errorf("define absolute module path: %w", err)
	}

	if err := verifyModulePath(moduleAbsRootPath); err != nil {
		return err
	}

	module, err := getModule(moduleAbsRootPath)
	if err != nil {
		return fmt.Errorf("define module path: %w", err)
	}

	prev, next, err := normaliseDesiredImportReplacement(module, fromVersion, toVersion)
	if err != nil {
		return err
	}

	if err := rewriteImports(moduleAbsRootPath, prev, next); err != nil {
		return fmt.Errorf("re-write go imports: %s", err)
	}

	if err := rewriteProto(moduleAbsRootPath, prev, next); err != nil {
		return fmt.Errorf("re-write .proto files: %s", err)
	}

	if err := rewriteGoMod(moduleAbsRootPath, next); err != nil {
		return fmt.Errorf("re-write go.mod file: %s", err)
	}

	return nil
}

func getInput() (dirLocation string, fromVersion string, toVersion string, err error) {
	flag.StringVar(&dirLocation, "dir", ".", "directory of the module with the go.mod file")
	flag.StringVar(&fromVersion, "from", "", "module version to upgrade from")
	flag.StringVar(&toVersion, "to", "", "module version to upgrade to")
	flag.Parse()
	return
}

func isModuleVersion(input string) error {
	if input == "" {
		return errors.New("empty module version")
	}

	if !strings.HasPrefix(input, "v") {
		return fmt.Errorf("module version should start with 'v': %s", input)
	}

	rawVersion, err := strconv.ParseInt(input[1:], 10, 64)
	if err != nil {
		return err
	}
	if rawVersion < 1 {
		return fmt.Errorf("version number should be positive: %d", rawVersion)
	}

	return nil
}

func getModule(modDir string) (string, error) {
	cmd := exec.Command("go", "mod", "edit", "-json")
	cmd.Dir = modDir
	data, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("command %q: %v", strings.Join(cmd.Args, " "), exitErr.Stderr)
		}
		return "", fmt.Errorf("command %q: %w", strings.Join(cmd.Args, " "), err)
	}

	var modInfo = struct{ Module struct{ Path string } }{}
	if err := json.Unmarshal(data, &modInfo); err != nil {
		return "", err
	}

	return modInfo.Module.Path, nil
}

// rewriteImports rewrites go source files by replacing old import path for the module with the new one.
func rewriteImports(moduleAbsRootPath, prev, next string) error {
	fileSet := token.NewFileSet()
	if err := filepath.Walk(moduleAbsRootPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			relPath := strings.TrimPrefix(path, moduleAbsRootPath)
			relPath = strings.TrimPrefix(relPath, "/")
			if skipDirs[relPath] {
				return fs.SkipDir
			}
			return nil
		}

		if filepath.Ext(info.Name()) == ".go" {
			fileSet.AddFile(path, fileSet.Base(), 0)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("scan directory for go source files: %w", err)
	}

	seen := map[string]struct{}{}
	var rerr error
	fileSet.Iterate(func(file *token.File) bool {
		if _, found := seen[file.Name()]; found {
			return false
		}
		seen[file.Name()] = struct{}{}

		astFile, err := parser.ParseFile(fileSet, file.Name(), nil, parser.ParseComments)
		if err != nil {
			rerr = err
			return false
		}

		for _, imprt := range astFile.Imports {
			oldImport, err := strconv.Unquote(imprt.Path.Value)
			if err != nil {
				rerr = fmt.Errorf("unquote import: %s :%w", imprt.Path.Value, err)
				return false
			}

			if newImport := strings.Replace(oldImport, prev, next, 1); newImport != oldImport {
				imprt.EndPos = imprt.End()
				imprt.Path.Value = strconv.Quote(newImport)
			}
		}

		f, err := os.Create(file.Name())
		if err != nil {
			rerr = fmt.Errorf("open file %q: %w", file.Name(), err)
			return false
		}

		ferr := format.Node(f, fileSet, astFile)
		cerr := f.Close()

		if ferr != nil {
			rerr = fmt.Errorf("rewrite file %q: %w", file.Name(), err)
			return false
		}

		if cerr != nil {
			rerr = fmt.Errorf("close file %q: %w", file.Name(), err)
			return false
		}

		return true
	})

	return rerr
}

func normaliseDesiredImportReplacement(module, from, to string) (string, string, error) {
	if err := isModuleVersion(from); err != nil {
		return "", "", fmt.Errorf("invalid 'from' version: %w", err)
	}

	if err := isModuleVersion(to); err != nil {
		return "", "", fmt.Errorf("invalid 'to' version: %w", err)
	}

	prev := module
	next := filepath.Join(filepath.Dir(module), to)
	current := filepath.Base(module)
	if err := isModuleVersion(current); err == nil {
		if current != from {
			return "", "", fmt.Errorf("existing module version is %q, but 'from' specified as %q", current, from)
		}
	} else {
		next = filepath.Join(module, to)
	}
	return prev, next, nil
}

func verifyModulePath(moduleRootPath string) error {
	st, err := os.Stat(moduleRootPath)
	if err != nil {
		return fmt.Errorf("inspect module root path: %w", err)
	}

	if !st.IsDir() {
		return fmt.Errorf("provided module root path is not a directory: %s", moduleRootPath)
	}

	entries, err := os.ReadDir(moduleRootPath)
	if err != nil {
		return fmt.Errorf("inspect module root path: %w", err)
	}

	var modExists bool
	for _, entry := range entries {
		if entry.Name() == "go.mod" {
			modExists = true
			break
		}
	}

	if !modExists {
		return fmt.Errorf("provided module root path doesn't contain go.mod file: %s", moduleRootPath)
	}

	return nil
}

// rewriteProto re-write proto files by changing the go_package option declaration:
// 1. option go_package = "gitlab.com/gitlab-org/gitaly/proto/go/gitalypb";
// 2. option go_package = "gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb";
// 4. option go_package = "gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb";
func rewriteProto(moduleAbsRootPath, prev, next string) error {
	protoDirPath := filepath.Join(moduleAbsRootPath, "proto")
	if err := filepath.Walk(protoDirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if info.Name() == "go" {
				return fs.SkipDir
			}
			return nil
		}

		if filepath.Ext(info.Name()) != ".proto" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		var modified [][]byte
		lines := bytes.Split(data, []byte{'\n'})
		for _, line := range lines {
			tokens := bytes.Fields(line)
			pckg := bytes.Join(tokens, []byte{'~'})
			if !bytes.HasPrefix(pckg, []byte(`option~go_package~=~"`+prev+`/proto/go/gitalypb";`)) {
				modified = append(modified, line)
				continue
			}

			modified = append(modified, bytes.ReplaceAll(line, []byte(prev), []byte(next)))
		}
		if len(modified) == 0 {
			return nil
		}
		modifiedData := bytes.Join(modified, []byte{'\n'})

		if err := os.WriteFile(path, modifiedData, info.Mode()); err != nil {
			return fmt.Errorf("write modified file content: %s, %w", info.Name(), err)
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

// rewriteGoMod modifies name of the module in the go.mod file.
func rewriteGoMod(moduleAbsRootPath, next string) error {
	cmd := exec.Command("go", "mod", "edit", "-module", next)
	cmd.Dir = moduleAbsRootPath
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command %q: %w", strings.Join(cmd.Args, " "), err)
	}

	return nil
}
