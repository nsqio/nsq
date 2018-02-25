package nsqd

import (
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/orm"
)

type NsqPublishLog struct {
	Id         int       `orm:"column(id);auto"`
	NsqdUrl    string    `orm:"column(nsqd_url);size(100);null"`
	Topic      string    `orm:"column(topic);size(100);null"`
	MessageId  string    `orm:"column(message_id);size(100);null"`
	Message    string    `orm:"column(message);size(2000);null"`
	CreateTime time.Time `orm:"column(create_time);type(datetime);null;auto_now_add"`
}

func init() {
	orm.RegisterModel(new(NsqPublishLog))
}

// Add NsqPublishLog
func AddNsqPublishLog(m *NsqPublishLog) (id int64, err error) {
	o := orm.NewOrm()
	id, err = o.Insert(m)
	if err != nil {
		beego.Error(err)
	}
	return
}

// Get NsqPublishLog by id
func GetNsqPublishLogById(key int) (v *NsqPublishLog, err error) {
	o := orm.NewOrm()
	v = &NsqPublishLog{}
	err = o.QueryTable(new(NsqPublishLog)).Filter("id", key).One(v)
	if err != nil && err != orm.ErrNoRows {
		beego.Error(err)
	}
	return v, err
}

// Get NsqPublishLog list by id
func GetNsqPublishLogList(key string) (list []*NsqPublishLog, err error) {
	o := orm.NewOrm()
	_, err = o.QueryTable(new(NsqPublishLog)).Filter("id", key).All(&list)
	if err != nil {
		beego.Error(err)
	}
	return list, err
}

// Update NsqPublishLog
func UpdateNsqPublishLog(m *NsqPublishLog) (err error) {
	o := orm.NewOrm()
	_, err = o.Update(m)
	if err != nil {
		beego.Error(err)
	}
	return
}

// Delete NsqPublishLog
func DeleteNsqPublishLog(pk int) (err error) {
	o := orm.NewOrm()
	v := NsqPublishLog{Id: pk}
	// ascertain id exists in the database
	_, err = o.Delete(&v)
	if err != nil {
		beego.Error(err)
	}
	return
}
