package flume_log

import (
	"errors"
)

type DetailInfo struct {
	module string
	detail map[string]interface{}
}

type ExtraInfo map[string]interface{}

func NewDetailInfo(m string) *DetailInfo {
	detailInfo := &DetailInfo{
		module: m,
		detail: make(map[string]interface{}),
	}
	return detailInfo
}

func (d *DetailInfo) AddExtraInfo(extraInfo ExtraInfo) {
	extraList := make([]ExtraInfo, 0)
	extraList = append(extraList, extraInfo)
	d.detail["extra"] = extraList
}

func (d *DetailInfo) AddLogItem(key string, value interface{}) error {
	var extraList []ExtraInfo
	var extraInfo ExtraInfo
	if extraObj, ok := d.detail["extra"]; ok {
		if extraList, ok = extraObj.([]ExtraInfo); !ok {
			return errors.New("extraObj.([]ExtraInfo) failed")
		}
	} else {
		extraList = make([]ExtraInfo, 0)
	}
	if len(extraList) > 0 {
		extraInfo = extraList[0]
	} else {
		extraInfo = make(map[string]interface{})
		extraList = append(extraList, extraInfo)
	}
	extraInfo[key] = value
	d.detail["extra"] = extraList
	return nil
}

func (d *DetailInfo) AddKeyValue(key string, value interface{}) {
	d.detail[key] = value
}
