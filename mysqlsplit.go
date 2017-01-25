package main

import (
	"bufio"
	"compress/gzip"
	//	"fmt"
	"github.com/op/go-logging"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strings"
	"sync"
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
			log.Warningf("Starting profiler at localhost:6060")
			log.Errorf("error running profiler: %s", http.ListenAndServe("localhost:6060", nil))
		}()
	}

	log.Info("Starting split from stdin, outdir is out/")
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
	// sync workers by waitgroup
	var wgEnd sync.WaitGroup
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
					delete(tables, currentTable)
				}
				currentTable = tableName
				tablesStarted = true
				tables[tableName] = FileWriter("out/"+tableName+".sql.gz", &wgEnd)
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
	// close off remaining tables
	log.Warning("waiting for workers to close")
	for k, v := range tables {
		log.Warningf("Closing %s", k)
		close(v)
		delete(tables, k)
	}
	// wait till stuff is actually saved and closed
	wgEnd.Wait()

}

func FileWriter(name string, wg *sync.WaitGroup) chan *string {
	ch := make(chan *string, 128)
	go func(f string, ch chan *string) {
		wg.Add(1)
		// set to done once we ended so app can wait for all files to be saved
		defer wg.Done()
		log.Noticef("Starting write to %s", name)
		file, err := os.Create(name)
		gzw := gzip.NewWriter(file)
		if err != nil {
			log.Errorf("file open error %s:%s", name, err)
		}
		for line := range ch {
			gzw.Write([]byte(*line))
		}
		log.Noticef("Closing %s", name)
		gzw.Close()
		file.Close() // gzip does not close writer

	}(name, ch)
	return ch
}
