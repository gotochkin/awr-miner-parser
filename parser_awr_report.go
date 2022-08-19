// Copyright 2022 Gleb Otochkin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var atpDB = map[string]string{
	"service":        "db1",
	"username":       "USERNAME",
	"server":         "hostname",
	"port":           "1522",
	"password":       "***************",
	"walletLocation": "$TNS_ADMIN/",
}

var db *sql.DB
var isUploadDB = flag.Bool("upload", true, "Upload to database")

// From Stathat blog https://blog.stathat.com/2012/10/10/time_any_function_in_go.html
func elapsedTime(t1 time.Time, fname string) {
	el := time.Since(t1)
	log.Printf("%s executed in %s", fname, el)
}

func os_info(lines int) {
	fmt.Print("Starting parsing OS information from line ", lines, "\n")

}

func patch_info(lines int) {
	fmt.Print("Starting parsing patching information from line ", lines, "\n")

}

func mem_info(lines int) {
	fmt.Print("Starting parsing memory information from line ", lines, "\n")

}

func sga_advice_info(lines int) {
	fmt.Print("Starting parsing sga advice information from line ", lines, "\n")

}

func pga_advice_info(lines int) {
	fmt.Print("Starting parsing PGA advice information from line ", lines, "\n")

}

func size_info(lines int) {
	fmt.Print("Starting parsing size on the disk information from line ", lines, "\n")

}

func osstat_info(lines int) {
	fmt.Print("Starting parsing os stat information from line ", lines, "\n")

}

func main_metrics_info(lines int) {
	fmt.Print("Starting parsing main metrics information from line ", lines, "\n")

}

const os_info_create_table = "create table OS_INFORMATION (file_id varchar2(200),stat_name varchar2(50),stat_value varchar2(50))"

func parse_section(sname string, fpath string, startln int, endln int) {
	//
	// connstr := "oracle://" + atpDB["username"] + ":" + atpDB["password"] + "@" + atpDB["server"] + ":" + atpDB["port"] + "/" + atpDB["service"]
	// // if val, ok := atpDB["walletLocation"]; ok && val != "" {
	// // 	connstr += "?TRACE FILE=/Users/otochkin/working/TNS_ADMIN/glebatp02/trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(atpDB["walletLocation"])
	// // }
	// //connstr = "oracle://" + atpDB["username"] + ":" + atpDB["password"] + "@glebatp02_tp"
	// fmt.Println(connstr)
	// conn, err := go_ora.NewConnection(connstr)
	// err = conn.Open()
	// if err != nil {
	// 	panic(fmt.Errorf("opening error %w", err))
	// }
	// defer func() {
	// 	err := conn.Close()
	// 	if err != nil {
	// 		panic(fmt.Errorf("closing error %w", err))
	// 	}
	// }()
	rf, err := os.Open(fpath)
	if err != nil {
		fmt.Print("Error opening the file: ", err)
	}
	defer rf.Close()
	lines := 0
	scan := bufio.NewScanner(rf)
	scan.Split(bufio.ScanLines)
	//println("here where we stand " + strconv.Itoa(startln) + " " + strconv.Itoa(endln))
	for scan.Scan() {
		//
		lines++
		if lines > startln && lines < endln {
			//
			if len(scan.Text()) > 0 && !strings.HasPrefix(scan.Text(), "--") {
				sdata := strings.Fields(scan.Text())
				fmt.Println(sdata)
			}
		}

	}
}

func prepareStmtTxt(t string, sdata []string) (inserttxt string, createtable string) {
	//Prepare the text fo the statement
	insertstart := "insert into " + t + "(" //id, rnd_str,use_date) values (?,?,?)"
	insertend := ") values("
	createtable = "CREATE TABLE " + t + " ("
	for _, v := range sdata {
		insertstart = insertstart + v + ","
		insertend = insertend + "?,"
		createtable = createtable + v + " varchar(100),"
	}
	insertend = strings.Replace(insertend+")", ",)", ")", 1)
	inserttxt = strings.Replace(insertstart+")", ",)", insertend, 1)
	createtable = strings.Replace(createtable+")", ",)", ")", 1)
	// stmt, err := db.Prepare(stmttxt)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer stmt.Close()
	// for i := 1; i <= 5; i++ {
	// 	if _, err := stmt.Exec(i, "Test "+strconv.Itoa(i), time.Now()); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	return inserttxt, createtable
}

func checkDBObject(dbname string, objname string) (int, error) {
	//
	defer elapsedTime(time.Now(), "chekObject")
	var cnt int
	err := db.QueryRow("select count(*) from information_schema.tables where table_schema=? and table_name=?", dbname, objname).Scan(&cnt)
	if err != nil {
		//return 0, fmt.Errorf("DB.QueryRow: %v", err)
		return -1, err
	}
	if cnt > 0 {
		return cnt, nil
	}
	return cnt, nil
}

func execStmt(tdll string) error {
	//
	_, err := db.Exec(tdll)
	return err
}

func parse_section_2(sname string, scan bufio.Scanner, startln int) {
	lines := 0
	//fmt.Println(scan.Text())
	for scan.Scan() {
		lines++
		if strings.Contains(scan.Text(), "~~END-") {
			break
		}
		inserttxt := ""
		createtable := ""

		if lines == startln-1 {
			sdata := strings.Fields(scan.Text())
			sdata = append(sdata, "MYFILENAME", "USEDATE")
			fmt.Println(sdata)
			// Prepare the insert
			inserttxt, createtable = prepareStmtTxt(sname, sdata)
		}
		fmt.Println(inserttxt)
		fmt.Println(createtable)

		//check if the table exists
		chk, chkerr := checkDBObject("testdb", sname)
		if chkerr != nil {
			log.Fatal(chkerr)
		}
		fmt.Println(chk)
		// if chk == 0 {
		// 	//
		// 	errddl := execStmt(createtable)
		// 	if errddl != nil {
		// 		log.Fatalf("Unable to create object: %s", errddl)
		// 	}
		// }
		//stmt, err := db.Prepare(stmttxt)
		//
		if lines > startln && len(scan.Text()) > 0 {
			sdata := strings.Fields(scan.Text())
			sdata = append(sdata, "MyFileName", time.Now().Format("2006/01/02T15:04:05"))
			for _, v := range sdata {
				fmt.Print(v + " ")
				//fmt.Print(i)
			}
			fmt.Print("\n")
			//fmt.Println(sdata)
		}
	}
	//fmt.Println(scan.Text())
}

func main() {
	//prepare initial variables
	lines := 0
	startln := 0
	endln := 10000
	maxSize := 4096
	//path to report file - testing only
	fpath := "upload/awr-hist-1288227953-ORCL-14815-14820.out"
	//parse input variables
	flag.Parse()
	//Check db connection
	//if *isUploadDB {
	//configuration
	mySQLcfg := mysql.Config{
		User:                 os.Getenv("DBUSER"),
		Passwd:               os.Getenv("DBPASS"),
		Net:                  "tcp",
		Addr:                 os.Getenv("DBHOST") + ":3306",
		DBName:               os.Getenv("DBNAME"),
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", mySQLcfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	t1 := time.Now()

	pingErr := db.Ping()
	el := time.Since(t1)
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	//el := time.Since(t1)
	fmt.Println("Connected!")
	fmt.Println(el)
	//}
	chk, chkerr := checkDBObject("testdb", "OS")
	if err != nil {
		log.Fatal(chkerr)
	}
	if chk == 0 {
		fmt.Println(chk)
	}

	//open report file
	rf, err := os.Open(fpath)
	if err != nil {
		fmt.Print("Error opening the file: ", err)
	}
	defer rf.Close()
	finf, err := rf.Stat()
	if err != nil {
		fmt.Print("Error getting information about the file: ", err)
	}
	maxSize = int(finf.Size())
	scan := bufio.NewScanner(rf)
	// Setting the buffer
	buf := make([]byte, 0, maxSize)
	scan.Buffer(buf, maxSize)
	//scan.Split(bufio.ScanLines)
	for scan.Scan() {
		lines++
		if len("~~BEGIN-OS-INFORMATION~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-OS-INFORMATION~~", scan.Text()) {
			os_info(lines)
			startln = 2
			fmt.Println(startln)
			//parse_section_2("OS", *scan, startln)
		}

		if len("~~BEGIN-PATCH-HISTORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-PATCH-HISTORY~~", scan.Text()) {
			patch_info(lines)
			startln = 3
			//parse_section_2("PATCH", *scan, startln)
		}

		if len("~~BEGIN-MEMORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY~~", scan.Text()) {
			mem_info(lines)
			startln = 3
			//parse_section_2("MEMORY", *scan, startln)
		}

		if len("~~BEGIN-MEMORY-SGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-SGA-ADVICE~~", scan.Text()) {
			sga_advice_info(lines)
			startln = 3
			//parse_section_2("SGA-ADVICE", *scan, startln)
		}

		if len("~~BEGIN-MEMORY-PGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-PGA-ADVICE~~", scan.Text()) {
			pga_advice_info(lines)
			startln = 3
			//parse_section_2("MEMORY-PGA-ADVICE", *scan, startln)
		}

		if len("~~BEGIN-SIZE-ON-DISK~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-SIZE-ON-DISK~~", scan.Text()) {
			size_info(lines)
			startln = 3
			//parse_section_2("SIZE-ON-DISK", *scan, startln)
		}

		if len("~~BEGIN-OSSTAT~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-OSSTAT~~", scan.Text()) {
			osstat_info(lines)
			startln = 3
			//parse_section_2("OSSTAT", *scan, startln)
		}

		if len("~~BEGIN-MAIN-METRICS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MAIN-METRICS~~", scan.Text()) {
			main_metrics_info(lines)
			startln = 3
			//parse_section_2("MAIN-METRICS", *scan, startln)
		}
		if len("~~BEGIN-DATABASE-PARAMETERS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-DATABASE-PARAMETERS~~", scan.Text()) {
			main_metrics_info(lines)
			startln = 3
			//parse_section_2("DATABASE-PARAMETERS", *scan, startln)
		}

		// line := scan.Bytes()
		// if len(line) <= 1 {
		// 	continue
		// }
		// if !bytes.Equal(line[1:25], []byte("~~BEGIN-OS-INFORMATION~~")) {
		// 	fmt.Print("Starting parsing OS information from ", lines, "\n")
		// 	fmt.Println(line[1:10])
		// }
		//fmt.Println(lines)
	}
	fmt.Println(endln)

}
