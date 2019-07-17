package praefect

import (
	"io/ioutil"
	"testing"

	"github.com/sirupsen/logrus"
)

var testLogger = logrus.New()

func init() {
	testLogger.SetOutput(ioutil.Discard)
}

func TestSecondaryRotation(t *testing.T) {
	t.Skip("secondary rotation will change with the new data model")
}
