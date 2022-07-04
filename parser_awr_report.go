package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var atpDB = map[string]string{
	"service":        "db1",
	"username":       "USERNAME",
	"server":         "hostname",
	"port":           "1522",
	"password":       "***************",
	"walletLocation": "$TNS_ADMIN/",
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
func parse_section_2(sname string, scan bufio.Scanner, startln int) {
	lines := 0
	//fmt.Println(scan.Text())
	for scan.Scan() {
		lines++
		if strings.Contains(scan.Text(), "~~END-") {
			break
		}
		if lines > startln && len(scan.Text()) > 0 {
			sdata := strings.Fields(scan.Text())
			fmt.Println(sdata)
		}
	}
	fmt.Println(scan.Text())
}

func main() {
	//prepare initial variables
	lines := 0
	startln := 0
	endln := 10000
	maxSize := 4096
	fpath := "upload/awr-hist-1288227953-ORCL-14815-14820.out"
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
			parse_section_2("OS", *scan, startln)
		}
		if len("~~END-OS-INFORMATION~~") == len(scan.Text()) && strings.EqualFold("~~END-OS-INFORMATION~~", scan.Text()) {
			endln = lines
			//parse_section("OS Information", fpath, startln, endln)
		}

		if len("~~BEGIN-PATCH-HISTORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-PATCH-HISTORY~~", scan.Text()) {
			patch_info(lines)
			startln = 3
			parse_section_2("PATCH", *scan, startln)
		}

		if len("~~END-PATCH-HISTORY~~") == len(scan.Text()) && strings.EqualFold("~~END-PATCH-HISTORY~~", scan.Text()) {
			endln = lines
			//parse_section("Patch Information", fpath, startln, endln)
		}

		if len("~~BEGIN-MEMORY~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY~~", scan.Text()) {
			mem_info(lines)
			startln = 3
			parse_section_2("MEMORY", *scan, startln)
		}

		if len("~~BEGIN-MEMORY-SGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-SGA-ADVICE~~", scan.Text()) {
			sga_advice_info(lines)
			startln = 3
			parse_section_2("SGA-ADVICE", *scan, startln)
		}

		if len("~~BEGIN-MEMORY-PGA-ADVICE~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MEMORY-PGA-ADVICE~~", scan.Text()) {
			pga_advice_info(lines)
			startln = 3
			parse_section_2("MEMORY-PGA-ADVICE", *scan, startln)
		}

		if len("~~BEGIN-SIZE-ON-DISK~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-SIZE-ON-DISK~~", scan.Text()) {
			size_info(lines)
			startln = 3
			parse_section_2("SIZE-ON-DISK", *scan, startln)
		}

		if len("~~BEGIN-OSSTAT~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-OSSTAT~~", scan.Text()) {
			osstat_info(lines)
			startln = 3
			parse_section_2("OSSTAT", *scan, startln)
		}

		if len("~~BEGIN-MAIN-METRICS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-MAIN-METRICS~~", scan.Text()) {
			main_metrics_info(lines)
			startln = 3
			parse_section_2("MAIN-METRICS", *scan, startln)
		}
		if len("~~BEGIN-DATABASE-PARAMETERS~~") == len(scan.Text()) && strings.EqualFold("~~BEGIN-DATABASE-PARAMETERS~~", scan.Text()) {
			main_metrics_info(lines)
			startln = 3
			parse_section_2("DATABASE-PARAMETERS", *scan, startln)
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
