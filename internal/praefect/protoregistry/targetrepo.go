package protoregistry

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	protobufTag      = "protobuf"
	protobufOneOfTag = "protobuf_oneof"
)

// reflectFindRepoTarget finds the target repository by using the OID to
// navigate the struct tags
// Warning: this reflection filled function is full of forbidden dark elf magic
func reflectFindRepoTarget(pbMsg proto.Message, targetOID []int) (*gitalypb.Repository, error) {
	var targetRepo *gitalypb.Repository

	msgV, err := traverseProtoMessage(pbMsg, targetOID)
	if err != nil {
		return nil, err
	}

	targetRepo, ok := msgV.Interface().(*gitalypb.Repository)
	if !ok {
		return nil, fmt.Errorf("repo target OID %v points to non-Repo type %+v", targetOID, msgV.Interface())
	}

	return targetRepo, nil
}

// reflectFindStringTarget finds the target string field by using the OID to
// navigate the struct tags
func reflectFindStringTarget(pbMsg proto.Message, targetOID []int) (string, error) {
	var targetStorage string

	msgV, err := traverseProtoMessage(pbMsg, targetOID)
	if err != nil {
		return "", err
	}

	targetStorage, ok := msgV.Interface().(string)
	if !ok {
		return "", fmt.Errorf("storage target OID 1 points to non-storage type %+v", msgV.Interface())
	}

	return targetStorage, nil
}

func traverseProtoMessage(pbMsg proto.Message, targetOID []int) (reflect.Value, error) {

	msgV := reflect.ValueOf(pbMsg)

	for _, fieldNo := range targetOID {
		var err error

		msgV, err = findProtoField(msgV, fieldNo)
		if err != nil {
			return reflect.Value{}, err
		}
	}

	return msgV, nil
}

// matches a tag string like "bytes,1,opt,name=repository,proto3"
var protobufTagRegex = regexp.MustCompile(`^(.*?),(\d+),(.*?),name=(.*?),proto3(\,oneof)?$`)

const (
	protobufTagRegexGroups     = 6
	protobufTagRegexFieldGroup = 2
)

func findProtoField(msgV reflect.Value, protoField int) (reflect.Value, error) {
	msgV = reflect.Indirect(msgV)
	for i := 0; i < msgV.NumField(); i++ {
		field := msgV.Type().Field(i)

		ok, err := tryNumberedField(field, protoField)
		if err != nil {
			return reflect.Value{}, err
		}
		if ok {
			return msgV.FieldByName(field.Name), nil
		}

		oneofField, ok := tryOneOfField(msgV, field, protoField)
		if !ok {
			continue
		}
		return oneofField, nil
	}

	err := fmt.Errorf(
		"unable to find protobuf field %d in message %s",
		protoField, msgV.Type().Name(),
	)
	return reflect.Value{}, err
}

func tryNumberedField(field reflect.StructField, protoField int) (bool, error) {
	tag := field.Tag.Get(protobufTag)
	matches := protobufTagRegex.FindStringSubmatch(tag)
	if len(matches) == protobufTagRegexGroups {
		fieldStr := matches[protobufTagRegexFieldGroup]
		if fieldStr == strconv.Itoa(protoField) {
			return true, nil
		}
	}

	return false, nil
}

func tryOneOfField(msgV reflect.Value, field reflect.StructField, protoField int) (reflect.Value, bool) {
	oneOfTag := field.Tag.Get(protobufOneOfTag)
	if oneOfTag == "" {
		return reflect.Value{}, false // empty tag means this is not a oneOf field
	}

	// try all of the oneOf fields until a match is found
	msgV = msgV.FieldByName(field.Name).Elem().Elem()
	for i := 0; i < msgV.NumField(); i++ {
		field = msgV.Type().Field(i)

		ok, err := tryNumberedField(field, protoField)
		if err != nil {
			return reflect.Value{}, false
		}
		if ok {
			return msgV.FieldByName(field.Name), true
		}
	}

	return reflect.Value{}, false
}
