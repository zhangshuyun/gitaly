package repocleaner

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestLogWarnAction_Perform(t *testing.T) {
	logger, hook := test.NewNullLogger()
	action := NewLogWarnAction(logger)
	err := action.Perform(context.TODO(), "vs1", "g1", []string{"p/1", "p/2"})
	require.NoError(t, err)
	require.Len(t, hook.AllEntries(), 2)

	exp := []map[string]interface{}{{
		"Data": logrus.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/1",
		},
		"Message": "repository is not managed by praefect",
	}, {
		"Data": logrus.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/2",
		},
		"Message": "repository is not managed by praefect",
	}}

	require.ElementsMatch(t, exp, []map[string]interface{}{{
		"Data":    hook.AllEntries()[0].Data,
		"Message": hook.AllEntries()[0].Message,
	}, {
		"Data":    hook.AllEntries()[1].Data,
		"Message": hook.AllEntries()[1].Message,
	}})
}
