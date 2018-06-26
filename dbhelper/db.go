package dbhelper

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/mucolud/lib/convert"
)

//ODB db
type ODB struct {
	db *sqlx.DB
}

//New 新建
func New(host, port, user, pwd, dbname string) *ODB {
	db := sqlx.MustOpen("mysql", fmt.Sprintf("%s:%s@(%s:%s)/%s?charset=utf8",
		user, pwd, host, port, dbname,
	))
	return &ODB{db}
}

//SetMaxIdleConns 设置最大链接数
func (ob *ODB) SetMaxIdleConns(num int) {
	ob.db.SetMaxIdleConns(num)
}

//SetMaxOpenConns 设置最大打开数
func (ob *ODB) SetMaxOpenConns(num int) {
	ob.db.SetMaxOpenConns(num)
}

//Close 关闭数据库
func (ob *ODB) Close() error {
	return ob.db.Close()
}

//Get 获取一个数据
func (ob *ODB) Get(sql string, dst interface{}, val ...interface{}) (err error) {
	return ob.db.Get(dst, sql, val...)
}

//Select 获取集合数据
func (ob *ODB) Select(sql string, dst interface{}, val ...interface{}) (err error) {
	return ob.db.Select(dst, sql, val...)
}

//SelectMap 获取集合数据
func (ob *ODB) SelectMap(sql string, val ...interface{}) (dst []map[string]interface{}, err error) {
	rows, err := ob.db.Queryx(sql, val...)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	dst = make([]map[string]interface{}, 0, 100)
	for rows.Next() {
		var vals = make([]interface{}, len(columns))
		var pvals = make([]interface{}, len(columns))
		var val = make(map[string]interface{})

		for k := range vals {
			vals[k] = &pvals[k]
		}
		rows.Scan(vals...)
		for k, c := range columns {
			val[c] = *vals[k].(*interface{})

			val[c] = convert.ToString(val[c])
		}
		dst = append(dst, val)
	}
	return
}

//Count 获取数量
func (ob *ODB) Count(sql string, val ...interface{}) (num int, err error) {
	var dst int
	err = ob.db.Get(&dst, sql, val...)
	return dst, err
}

//CountGroup 获取数量
func (ob *ODB) CountGroup(sql string, val ...interface{}) (num int, err error) {
	var dst []int
	err = ob.db.Select(&dst, sql, val...)
	return len(dst), err
}

//Insert 插入一条数据
func (ob *ODB) Insert(sql string, val ...interface{}) (id int64, err error) {
	rlt, err := ob.db.Exec(sql, val...)
	if err != nil {
		return 0, err
	}
	return rlt.LastInsertId()
}

//Delete 删除一条数据
func (ob *ODB) Delete(sql string, val ...interface{}) (res bool, err error) {
	rlt, err := ob.db.Exec(sql, val...)
	if err != nil {
		return false, err
	}
	r, err := rlt.RowsAffected()
	if err != nil {
		return false, err
	}
	return r > 0, err
}

//Update 更新一条数据
func (ob *ODB) Update(sql string, val ...interface{}) (res bool, err error) {
	rlt, err := ob.db.Exec(sql, val...)
	if err != nil {
		return false, err
	}
	r, err := rlt.RowsAffected()
	if err != nil {
		return false, err
	}
	return r > 0, err
}
