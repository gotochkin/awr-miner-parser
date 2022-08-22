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
	"path/filepath"
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
var (
	isUploadDB = flag.Bool("upload", true, "Upload to database")
	fpath      = flag.String("fpath", "upload/awr-hist-1288227953-ORCL-14815-14820.out", "Path to the report file")
)

// From Stathat blog https://blog.stathat.com/2012/10/10/time_any_function_in_go.html
func elapsedTime(t1 time.Time, fname string) {
	el := time.Since(t1)
	log.Printf("%s executed in %s", fname, el)
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
		insertstart = insertstart + "`" + strings.ToUpper(v) + "`,"
		insertend = insertend + "?,"
		createtable = createtable + "`" + strings.ToUpper(v) + "` varchar(150),"
	}
	insertend = strings.Replace(insertend+")", ",)", ")", 1)
	inserttxt = strings.Replace(insertstart+")", ",)", insertend, 1)
	createtable = strings.Replace(createtable+")", ",)", ")", 1)

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

func parse_section_2(sname string, scan bufio.Scanner, startln int, fname string) {
	lines := 0
	var stmt *sql.Stmt
	var cdata []string
	for scan.Scan() {
		lines++
		if strings.Contains(scan.Text(), "~~END-") {
			break
		}
		inserttxt := ""
		createtable := ""

		if lines == startln-1 {
			sdata := strings.Fields(scan.Text())
			sdata = append(sdata, "FILENAME", "USEDATE")
			fmt.Println(sdata)
			// Prepare the insert
			inserttxt, createtable = prepareStmtTxt(sname, sdata)
			fmt.Println(inserttxt)
			fmt.Println(createtable)
			//check if the table exists
			chk, chkerr := checkDBObject("testdb", sname)
			if chkerr != nil {
				log.Fatal(chkerr)
			}
			// Purge data for the same filename statement
			purgestmt := "DELETE FROM `" + strings.ToUpper(sname) + "`"
			fmt.Println(chk)
			if chk == 0 {
				//
				errddl := execStmt(createtable)
				if errddl != nil {
					log.Fatalf("Unable to create object: %s", errddl)
				}
			} else {
				//Delete the data to avoid duplicates from previous unsuccessful imports
				errpurge := execStmt(purgestmt)
				if errpurge != nil {
					log.Fatal(errpurge)
				}
			}

			var stmterr error
			stmt, stmterr = db.Prepare(inserttxt)
			if stmterr != nil {
				log.Fatal(stmterr)
			}
			defer stmt.Close()
		}

		// Get the columns length
		if lines == startln {
			cdata = strings.Split(scan.Text(), " ")
		}

		//Fill the table
		if lines > startln && len(scan.Text()) > 0 {
			sdata := []string{}
			ind := 0
			for _, i := range cdata {
				//
				if len(scan.Text()) >= len(i)+ind {
					//
					sdata = append(sdata, scan.Text()[ind:len(i)+ind])
					ind = ind + len(i) + 1
				} else {
					//For the last column
					if len(scan.Text()) >= ind {
						sdata = append(sdata, scan.Text()[ind:len(scan.Text())])
					} else {
						sdata = append(sdata, "")
					}
				}
			}
			sdata = append(sdata, fname, time.Now().Format("2006/01/02T15:04:05"))
			//fmt.Printf("Fields are: %q", sdata)
			vargs := []any{}
			for _, v := range sdata {
				vargs = append(vargs, strings.Trim(v, " "))
			}
			if _, execerr := stmt.Exec(vargs...); execerr != nil {
				log.Fatal(execerr)
			}

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
	//fpath := "upload/awr-hist-1288227953-ORCL-14815-14820.out"
	//parse input variables
	flag.Parse()
	//Check db connection
	if *isUploadDB {
		//configuration
		mySQLcfg := mysql.Config{
			User:                 os.Getenv("DBUSER"),
			Passwd:               os.Getenv("DBPASS"),
			Net:                  "tcp",
			Addr:                 os.Getenv("DBHOST") + ":3306",
			DBName:               os.Getenv("DBNAME"),
			AllowNativePasswords: true,
		}
		var dberr error
		db, dberr = sql.Open("mysql", mySQLcfg.FormatDSN())
		if dberr != nil {
			log.Fatal(dberr)
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
	}

	//open report file
	rf, err := os.Open(*fpath)
	if err != nil {
		fmt.Print("Error opening the file: ", err)
	}
	defer rf.Close()
	finf, err := rf.Stat()
	if err != nil {
		fmt.Print("Error getting information about the file: ", err)
	}
	fa, err := filepath.Abs(*fpath)
	if err != nil {
		log.Fatal(err)
	}
	fname := filepath.Base(fa)
	maxSize = int(finf.Size())
	scan := bufio.NewScanner(rf)
	// Setting the buffer
	buf := make([]byte, 0, maxSize)
	scan.Buffer(buf, maxSize)
	//scan.Split(bufio.ScanLines)
	for scan.Scan() {
		lines++
		if len("~~BEGIN-OS-INFORMATION~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-OS-INFORMATION~~", scan.Text()) {
			fmt.Print("Starting parsing OS-INFORMATION information from line ", lines, "\n")
			startln = 2
			parse_section_2("OS", *scan, startln, fname)
		}

		if len("~~BEGIN-PATCH-HISTORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-PATCH-HISTORY~~", scan.Text()) {
			fmt.Print("Starting parsing PATCH-HISTORY information from line ", lines, "\n")
			startln = 3
			parse_section_2("PATCH", *scan, startln, fname)
		}

		if len("~~BEGIN-MEMORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY~~", scan.Text()) {
			fmt.Print("Starting parsing MEMORY information from line ", lines, "\n")
			startln = 3
			parse_section_2("MEMORY", *scan, startln, fname)
		}

		if len("~~BEGIN-MEMORY-SGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-SGA-ADVICE~~", scan.Text()) {
			fmt.Print("Starting parsing MEMORY-SGA-ADVICE information from line ", lines, "\n")
			startln = 3
			parse_section_2("MEMORY_SGA_ADVICE", *scan, startln, fname)
		}

		if len("~~BEGIN-MEMORY-PGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-PGA-ADVICE~~", scan.Text()) {
			fmt.Print("Starting parsing MEMORY-PGA-ADVICE information from line ", lines, "\n")
			startln = 3
			parse_section_2("MEMORY_PGA_ADVICE", *scan, startln, fname)
		}

		if len("~~BEGIN-SIZE-ON-DISK~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-SIZE-ON-DISK~~", scan.Text()) {
			fmt.Print("Starting parsing SIZE-ON-DISK information from line ", lines, "\n")
			startln = 3
			parse_section_2("SIZE_ON_DISK", *scan, startln, fname)
		}

		if len("~~BEGIN-OSSTAT~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-OSSTAT~~", scan.Text()) {
			fmt.Print("Starting parsing OSSTAT information from line ", lines, "\n")
			startln = 3
			parse_section_2("OSSTAT", *scan, startln, fname)
		}

		if len("~~BEGIN-MAIN-METRICS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MAIN-METRICS~~", scan.Text()) {
			fmt.Print("Starting parsing DATABASE-PARAMETERS information from line ", lines, "\n")
			startln = 3
			parse_section_2("MAIN_METRICS", *scan, startln, fname)
		}
		if len("~~BEGIN-DATABASE-PARAMETERS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-DATABASE-PARAMETERS~~", scan.Text()) {
			fmt.Print("Starting parsing DATABASE-PARAMETERS information from line ", lines, "\n")
			startln = 3
			parse_section_2("DATABASE_PARAMETERS", *scan, startln, fname)
		}
		if len("~~BEGIN-AVERAGE-ACTIVE-SESSIONS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-AVERAGE-ACTIVE-SESSIONS~~", scan.Text()) {
			fmt.Print("Starting parsing AVERAGE-ACTIVE-SESSIONS information from line ", lines, "\n")
			startln = 3
			parse_section_2("AVERAGE_ACTIVE_SESSIONS", *scan, startln, fname)
		}
		if len("~~BEGIN-IO-WAIT-HISTOGRAM~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-IO-WAIT-HISTOGRAM~~", scan.Text()) {
			fmt.Print("Starting parsingIO-WAIT-HISTOGRAM information from line ", lines, "\n")
			startln = 3
			parse_section_2("IO_WAIT_HISTOGRAM", *scan, startln, fname)
		}
		if len("~~BEGIN-IO-OBJECT-TYPE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-IO-OBJECT-TYPE~~", scan.Text()) {
			fmt.Print("Starting parsing IO-OBJECT-TYPE information from line ", lines, "\n")
			startln = 3
			parse_section_2("IO_OBJECT_TYPE", *scan, startln, fname)
		}
		if len("~~BEGIN-IOSTAT-BY-FUNCTION~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-IOSTAT-BY-FUNCTION~~", scan.Text()) {
			fmt.Print("Starting parsing IOSTAT-BY-FUNCTION information from line ", lines, "\n")
			startln = 3
			parse_section_2("IOSTAT_BY_FUNCTION", *scan, startln, fname)
		}
		if len("~~BEGIN-TOP-N-TIMED-EVENTS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-TOP-N-TIMED-EVENTS~~", scan.Text()) {
			fmt.Print("Starting parsing TOP-N-TIMED-EVENTS information from line ", lines, "\n")
			startln = 3
			parse_section_2("TOP_N_TIMED_EVENTS", *scan, startln, fname)
		}
		if len("~~BEGIN-SYSSTAT~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-SYSSTAT~~", scan.Text()) {
			fmt.Print("Starting parsing SYSSTAT information from line ", lines, "\n")
			startln = 3
			parse_section_2("SYSSTAT", *scan, startln, fname)
		}
		if len("~~BEGIN-TOP-SQL-SUMMARY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-TOP-SQL-SUMMARY~~", scan.Text()) {
			fmt.Print("Starting parsing TOP-SQL-SUMMARY information from line ", lines, "\n")
			startln = 3
			parse_section_2("TOP_SQL_SUMMARY", *scan, startln, fname)
		}
		if len("~~BEGIN-TTOP-SQL-BY-SNAPID~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-TOP-SQL-BY-SNAPID~~", scan.Text()) {
			fmt.Print("Starting parsing TOP-SQL-BY-SNAPID information from line ", lines, "\n")
			startln = 3
			parse_section_2("TOP_SQL_BY_SNAPID", *scan, startln, fname)
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
