package ext

import (
	"regexp"
	"fmt"
)

const (
	E_EXT_NOT_SUPPORT = "E_EXT_NOT_SUPPORT"
	E_BAD_TAG	= "E_BAD_TAG"
	E_INVALID_JSON_HEADER = "E_INVALID_JSON_HEADER"
)

var MAX_TAG_LEN = 100
var validTagFmt = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

type ExtVer uint8

//ext versions
// version for message has no ext
var NO_EXT_VER = ExtVer(uint8(0))
// version for message has tag ext
var TAG_EXT_VER = ExtVer(uint8(2))
// version for message has json header ext
var JSON_HEADER_EXT_VER = ExtVer(uint8(4))


var noExt = NoExt{}
type NoExt []byte

func NewNoExt() NoExt {
	return noExt
}

func (n NoExt) ExtVersion() ExtVer {
	return NO_EXT_VER
}

func (n NoExt) GetBytes() []byte {
	return nil
}

var CLEINT_DISPATCH_TAG_KEY = "##client_dispatch_tag"

type JsonHeaderExt struct {
	bytes []byte
}

func NewJsonHeaderExt() *JsonHeaderExt {
	return &JsonHeaderExt{}
}

func (self *JsonHeaderExt) SetJsonHeaderBytes(jsonExtBytes []byte) {
	self.bytes = jsonExtBytes[0:]
}

func (self *JsonHeaderExt) ExtVersion() ExtVer {
	return JSON_HEADER_EXT_VER
}

func (self *JsonHeaderExt) GetBytes() []byte {
	return self.bytes
}

type TagExt []byte

func NewTagExt(tagName []byte) (TagExt, error) {
	if !validateTag(tagName) {
		return nil, fmt.Errorf("invalid tag %v", tagName)
	}
	return TagExt(tagName), nil
}

func (tag TagExt) GetTagName() string {
	return string(tag)
}

//pass in []byte not nil
func validateTag(beValidated []byte) bool {
	if len(beValidated) > MAX_TAG_LEN {
		return false
	}
	return validTagFmt.Match(beValidated)
}

func (tag TagExt) ExtVersion() ExtVer {
	return TAG_EXT_VER
}

func (tag TagExt) GetBytes() []byte {
	return tag
}

func (tag TagExt) String() string {
	return string(tag)
}

type IExtContent interface {
	ExtVersion() ExtVer
	GetBytes()   []byte
}