package logsanitizer

import (
	"regexp"

	"github.com/sirupsen/logrus"
)

// Pattern taken from Regular Expressions Cookbook, slightly modified though
//                                        |Scheme                |User                         |Named/IPv4 host|IPv6+ host
var hostPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+\-.]*://)?([a-z0-9\-._~%!$&'()*+,;=:]+@)([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])`)

// URLSanitizerHook stores which gRPC methods to perform sanitization for.
type URLSanitizerHook struct {
	// gRPC methods that are likely to have URLs to sanitize
	possibleGrpcMethods map[string]bool
}

// NewURLSanitizerHook returns a new logrus hook for sanitizing URLs.
func NewURLSanitizerHook() *URLSanitizerHook {
	return &URLSanitizerHook{possibleGrpcMethods: make(map[string]bool)}
}

// AddPossibleGrpcMethod adds method names that we should sanitize URLs from their logs.
func (hook *URLSanitizerHook) AddPossibleGrpcMethod(methods ...string) {
	for _, method := range methods {
		hook.possibleGrpcMethods[method] = true
	}
}

// Fire is called by logrus.
func (hook *URLSanitizerHook) Fire(entry *logrus.Entry) error {
	mth, ok := entry.Data["grpc.method"]
	if !ok {
		return nil
	}

	mthStr, ok := mth.(string)
	if !ok || !hook.possibleGrpcMethods[mthStr] {
		return nil
	}

	if _, ok := entry.Data["args"]; ok {
		sanitizeSpawnLog(entry)
	} else if _, ok := entry.Data["error"]; ok {
		sanitizeErrorLog(entry)
	} else {
		entry.Message = sanitizeString(entry.Message)
	}

	return nil
}

// Levels is called by logrus.
func (hook *URLSanitizerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func sanitizeSpawnLog(entry *logrus.Entry) {
	args, ok := entry.Data["args"].([]string)
	if !ok {
		return
	}

	for i, arg := range args {
		args[i] = sanitizeString(arg)
	}

	entry.Data["args"] = args
}

func sanitizeErrorLog(entry *logrus.Entry) {
	err, ok := entry.Data["error"].(error)
	if !ok {
		return
	}

	entry.Data["error"] = sanitizeString(err.Error())
}

func sanitizeString(str string) string {
	return hostPattern.ReplaceAllString(str, "$1[FILTERED]@$3$4")
}
