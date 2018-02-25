package nsqd

import (
	"time"

	"github.com/astaxie/beego"

	"github.com/astaxie/beego/orm"
)

type NsqConsumeLog struct {
	Id         int       `orm:"column(id);auto"`
	NsqdUrl    string    `orm:"column(nsqd_url);size(100);null"`
	MessageId  string    `orm:"column(message_id);size(45);null"`
	Topic      string    `orm:"column(topic);size(100);null"`
	Channel    string    `orm:"column(channel);size(100);null"`
	Message    string    `orm:"column(message);size(1000);null"`
	CreateTime time.Time `orm:"column(create_time);type(datetime);null;auto_now_add"`
}

func init() {
	orm.RegisterModel(new(NsqConsumeLog))
}

// Add NsqConsumeLog
func AddNsqConsumeLog(m *NsqConsumeLog) (id int64, err error) {
	o := orm.NewOrm()
	id, err = o.Insert(m)
	if err != nil {
		beego.Error(err)
	}
	return
}

// Get NsqConsumeLog by id
func GetNsqConsumeLogById(key int) (v *NsqConsumeLog, err error) {
	o := orm.NewOrm()
	v = &NsqConsumeLog{}
	err = o.QueryTable(new(NsqConsumeLog)).Filter("id", key).One(v)
	if err != nil && err != orm.ErrNoRows {
		beego.Error(err)
	}
	return v, err
}

// Get NsqConsumeLog list by id
func GetNsqConsumeLogList(key string) (list []*NsqConsumeLog, err error) {
	o := orm.NewOrm()
	_, err = o.QueryTable(new(NsqConsumeLog)).Filter("id", key).All(&list)
	if err != nil {
		beego.Error(err)
	}
	return list, err
}

// Update NsqConsumeLog
func UpdateNsqConsumeLog(m *NsqConsumeLog) (err error) {
	o := orm.NewOrm()
	_, err = o.Update(m)
	if err != nil {
		beego.Error(err)
	}
	return
}

// Delete NsqConsumeLog
func DeleteNsqConsumeLog(pk int) (err error) {
	o := orm.NewOrm()
	v := NsqConsumeLog{Id: pk}
	// ascertain id exists in the database
	_, err = o.Delete(&v)
	if err != nil {
		beego.Error(err)
	}
	return
}
