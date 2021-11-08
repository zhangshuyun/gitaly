package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

func TestCheckSubcommand_Exec(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc           string
		checks         []praefect.CheckFunc
		expectedOutput string
		expectedError  error
	}{
		{
			desc: "all checks pass",
			checks: []praefect.CheckFunc{
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 1",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 2",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 3",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
			},
			expectedOutput: "Checking check 1...Passed\nChecking check 2...Passed\nChecking check 3...Passed\n\nAll checks passed.\n",
			expectedError:  nil,
		},
		{
			desc: "a fatal check fails",
			checks: []praefect.CheckFunc{
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 1",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 2",
						Run:      func(ctx context.Context) error { return errors.New("i failed") },
						Severity: praefect.Fatal,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 3",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
			},
			expectedOutput: "Checking check 1...Passed\nChecking check 2...Failed (fatal) error: i failed\nChecking check 3...Passed\n\n1 check(s) failed, at least one was fatal.\n",
			expectedError:  errFatalChecksFailed,
		},
		{
			desc: "only warning checks fail",
			checks: []praefect.CheckFunc{
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 1",
						Run:      func(ctx context.Context) error { return nil },
						Severity: praefect.Fatal,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 2",
						Run:      func(ctx context.Context) error { return errors.New("i failed but not too badly") },
						Severity: praefect.Warning,
					}
				},
				func(cfg config.Config) *praefect.Check {
					return &praefect.Check{
						Name:     "check 3",
						Run:      func(ctx context.Context) error { return errors.New("i failed but not too badly") },
						Severity: praefect.Warning,
					}
				},
			},
			expectedOutput: "Checking check 1...Passed\nChecking check 2...Failed (warning) error: i failed but not too badly\nChecking check 3...Failed (warning) error: i failed but not too badly\n\n2 check(s) failed, but none are fatal.\n",
			expectedError:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var cfg config.Config
			var stdout bytes.Buffer

			checkCmd := checkSubcommand{w: &stdout, checkFuncs: tc.checks}

			assert.Equal(t, tc.expectedError, checkCmd.Exec(flag.NewFlagSet("", flag.PanicOnError), cfg))
			assert.Equal(t, tc.expectedOutput, stdout.String())
		})
	}
}
