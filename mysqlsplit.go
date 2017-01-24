package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strings"
)

var version string
var log = logging.MustGetLogger("main")
var debug = false
var stdout_log_format = logging.MustStringFormatter("%{color:bold}%{time:2006-01-02T15:04:05.9999Z-07:00}%{color:reset}%{color} [%{level:.1s}] %{color:reset}%{shortpkg}[%{longfunc}] %{message}")

var reExtractTableName = regexp.MustCompile("^-- Table structure for table `(.*)`")

func main() {
	stderrBackend := logging.NewLogBackend(os.Stderr, "", 0)
	stderrFormatter := logging.NewBackendFormatter(stderrBackend, stdout_log_format)
	logging.SetBackend(stderrFormatter)
	logging.SetFormatter(stdout_log_format)
	if debug {
		go func() {
			log.Warningf("profiler running at localhost:6060")
			log.Errorf("error running profiler: %s", http.ListenAndServe("localhost:6060", nil))
		}()
	}

	log.Info("Starting app")
	log.Debugf("version: %s", version)

	tables := make(map[string]chan *string)
	prefix := []string{
		"SET FOREIGN_KEY_CHECKS = 0;\n",
		"SET UNIQUE_CHECKS = 0;\n",
		"SET AUTOCOMMIT = 0;\n",
	}
	postfix := []string{
		"SET UNIQUE_CHECKS = 1;\n",
		"SET FOREIGN_KEY_CHECKS = 1;\n",
		"COMMIT;\n",
	}

	os.Mkdir(`out`, 0755)
	stdin := bufio.NewReader(os.Stdin)
	tablesStarted := false
	currentTable := ""
	// bufio.Scanner doesnt dynamically allocate buffers so it is useless for long lines
	for {
		line, readErr := stdin.ReadString('\n')
		if strings.HasPrefix(line, `-- Table structure`) {
			match := reExtractTableName.FindStringSubmatch(line)
			if len(match) > 1 {
				tableName := match[1]
				if len(currentTable) > 0 {
					for _, l := range postfix {
						str := l
						tables[currentTable] <- &str
					}
					close(tables[currentTable])
				}
				currentTable = tableName
				tablesStarted = true
				tables[tableName] = FileWriter("out/" + tableName + ".sql.gz")
				for _, l := range prefix {
					str := l
					tables[tableName] <- &str
				}
				log.Noticef("Found table %s\n", tableName)
			} else {
				log.Warningf("Failed to match %20s", line)
			}
		}
		if tablesStarted {
			tables[currentTable] <- &line
		} else {
			prefix = append(prefix, line)
		}
		if readErr == io.EOF {
			break
		} else if readErr != nil {
			log.Errorf("Read error: %s", readErr)
			break
		}
	}
	fmt.Println("end")
}

func FileWriter(name string) chan *string {
	ch := make(chan *string, 128)
	go func(f string, ch chan *string) {
		log.Noticef("Starting write to %s", name)
		file, err := os.Create(name)
		gzw := gzip.NewWriter(file)
		defer gzw.Close()
		if err != nil {
			log.Errorf("file open error %s:%s", name, err)
		}
		for line := range ch {
			gzw.Write([]byte(*line))
		}
		log.Noticef("Closing %s", name)
	}(name, ch)
	return ch
}
