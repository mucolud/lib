package dbhelper

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/mucolud/lib/convert"
)

//SQLHelper sql构建
type SQLHelper struct {
	sqlstr    string
	table     string
	whereVals []interface{}
}

//New 创建
func New(t string) *SQLHelper {
	return &SQLHelper{table: t}
}

//Clear 清空sql
func (sh *SQLHelper) Clear() *SQLHelper {
	sh.sqlstr = ""
	return sh
}

//Select select
func (sh *SQLHelper) Select(fields []string) *SQLHelper {
	var tsql = fmt.Sprintf("select %s from %s", strings.Join(fields, ","), sh.table)
	sh.sqlstr = tsql
	return sh
}

//Limit 构成limit语句
func (sh *SQLHelper) Limit(page, limit int) *SQLHelper {
	sh.sqlstr = sh.sqlstr + fmt.Sprintf(" limit %d,%d", (page-1)*limit, limit)
	return sh
}

//Order 构成order语句
func (sh *SQLHelper) Order(order string) *SQLHelper {
	sh.sqlstr = sh.sqlstr + fmt.Sprintf(" order by %s", order)
	return sh
}

//Group 构成group语句
func (sh *SQLHelper) Group(order string) *SQLHelper {
	sh.sqlstr = sh.sqlstr + fmt.Sprintf(" group by %s", order)
	return sh
}

//WhereStr 构成where语句
func (sh *SQLHelper) WhereStr(where string) *SQLHelper {
	sh.sqlstr = sh.sqlstr + fmt.Sprintf("where %s", where)
	return sh
}

//Count 构成count语句
func (sh *SQLHelper) Count(where string) *SQLHelper {
	if where == "" {
		where = "1=1"
	}
	sh.sqlstr = fmt.Sprintf("select count(*) as num from %s where %s", sh.table, where)
	return sh
}

//Insert 构成insert语句
func (sh *SQLHelper) Insert(fields []string) *SQLHelper {
	sh.sqlstr = fmt.Sprintf("insert into %s(%s) values(%s)", sh.table, strings.Join(fields, ","),
		strings.TrimRight(strings.Repeat("?,", len(fields)), ","))
	return sh
}

//Update 构成update语句
func (sh *SQLHelper) Update(fields []string) *SQLHelper {
	sh.sqlstr = fmt.Sprintf("update %s set %s", sh.table,
		strings.Join(fields, "=?,")+"=?")
	return sh
}

//Delete 构成delete语句
func (sh *SQLHelper) Delete() *SQLHelper {
	sh.sqlstr = fmt.Sprintf("delete from %s ", sh.table)
	return sh
}

//Where 构建where查询语句
func (sh *SQLHelper) Where(condition map[string]interface{}) *SQLHelper {
	w := "1=1"
	r := regexp.MustCompile(`\[(.*)\]`)
	vals := make([]interface{}, 0, len(condition))

	for k, v := range condition {
		//判断是否为空值
		if vs := convert.ToString(v); vs == "" || vs == "0" {
			continue
		}
		if vs := convert.ToString(v); strings.Contains(vs, "!!") {
			v = strings.Replace(vs, "!!", "", -1)
		}

		if rule := r.FindString(k); rule != "" {
			okey := strings.Replace(k, rule, "", -1)
			switch rule {
			case "[like]":
				w += fmt.Sprintf(" and %s like '%%?%%'", okey)
			case "[>=]":
				w += fmt.Sprintf(" and %s >= ?", okey)
			case "[>]":
				w += fmt.Sprintf(" and %s > ?", okey)
			case "[<=]":
				w += fmt.Sprintf(" and %s <= ?", okey)
			case "[<]":
				w += fmt.Sprintf(" and %s < ?", okey)
			}
		} else {
			w += fmt.Sprintf(" and %s = ?", k)
		}
		vals = append(vals, v)
	}
	sh.sqlstr = sh.sqlstr + " where " + w
	sh.whereVals = vals
	return sh
}

//String 返回sql语句
func (sh *SQLHelper) String() (string, []interface{}) {
	return sh.sqlstr, sh.whereVals
}

//SQL 返回sql语句
func (sh *SQLHelper) SQL() string {
	return sh.printSQL(sh.sqlstr, sh.whereVals...)
}

// 打印完整sql
func (sh *SQLHelper) printSQL(sql string, values ...interface{}) string {
	var isPrintable = func(s string) bool {
		for _, r := range s {
			if !unicode.IsPrint(r) {
				return false
			}
		}
		return true
	}
	var formattedValues []string
	sqlRegexp := regexp.MustCompile(`(\$\d+)|\?`)
	for _, value := range values {
		indirectValue := reflect.Indirect(reflect.ValueOf(value))
		if indirectValue.IsValid() {
			value = indirectValue.Interface()
			if t, ok := value.(time.Time); ok {
				formattedValues = append(formattedValues, fmt.Sprintf("'%v'", t.Format(time.RFC3339)))
			} else if b, ok := value.([]byte); ok {
				if str := string(b); isPrintable(str) {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", str))
				} else {
					formattedValues = append(formattedValues, "'<binary>'")
				}
			} else if r, ok := value.(driver.Valuer); ok {
				if value, err := r.Value(); err == nil && value != nil {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", value))
				} else {
					formattedValues = append(formattedValues, "NULL")
				}
			} else {
				formattedValues = append(formattedValues, fmt.Sprintf("'%v'", value))
			}
		} else {
			formattedValues = append(formattedValues, fmt.Sprintf("'%v'", value))
		}
	}

	var formattedValuesLength = len(formattedValues)
	var nsql = ""
	for index, value := range sqlRegexp.Split(sql, -1) {
		nsql += value
		if index < formattedValuesLength {
			nsql += formattedValues[index]
		}
	}

	return nsql
}
