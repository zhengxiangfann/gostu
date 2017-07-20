package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// MYSQLDB 全局数据库连接
var MYSQLDB *sql.DB

func main00n() {
	fmt.Println("Start clean data")
	var err error
	MYSQLDB, err = sql.Open("mysql", "root:123456@tcp(192.168.100.100:3306)/test?charset=utf8")
	defer MYSQLDB.Close()
	if err != nil {
		fmt.Println(err)
	}
	CleanData()
}

// CleanData  清理数据
func CleanData() {

	// var QuerySQL string
	QuerySQL := `SELECT count(*) vcount,companyOrgType FROM bigdata.big_enterprise_info where companyOrgType !='' group by companyOrgType`

	UpDateSQL := `update big_enterprise_info set   companyOrgType =? where companyOrgType =?`

	row, err := MYSQLDB.Query(QuerySQL)
	if err != nil {
		fmt.Println(err)
	}

	for row.Next() {

		var VCount int
		var OrgType string
		err := row.Scan(&VCount, &OrgType)

		if err != nil {
			continue
		}
		var NewOrgType string
		fmt.Printf("OrgType = %s, VCount = %d\r\n", OrgType, VCount)
		if strings.Contains(OrgType, "（") || strings.Contains(OrgType, "）") {
			NewOrgType = strings.Replace(strings.Replace(OrgType, "（", "(", -1), "）", ")", -1)
			stmt, _ := MYSQLDB.Prepare(UpDateSQL)
			res, _ := stmt.Exec(NewOrgType, OrgType)
			num, _ := res.RowsAffected()
			fmt.Printf("将[%s]更新为[%s]更新了[%d]行\r\n", OrgType, NewOrgType, num)
		}
	}
	row.Close()
}
