package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/urfave/cli"

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
var compress = true
var stdout_log_format = logging.MustStringFormatter("%{color:bold}%{time:2006-01-02T15:04:05.0000Z-07:00}%{color:reset}%{color} [%{level:.1s}] %{color:reset}%{shortpkg}[%{longfunc}] %{message}")

var reExtractTableName = regexp.MustCompile("^-- Table structure for table `(.*)`")

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:        "debug",
			Usage:       "enables debug",
			Destination: &debug,
		},
		cli.BoolTFlag{
			Name:  "add-check-off-prefix",
			Usage: "disables foreign key checks on load (enabled by default)",
		},
		cli.BoolTFlag{
			Name:  "add-check-on-suffix",
			Usage: "enables foreign key checks on end of file (enabled by default)",
		},
		cli.BoolTFlag{
			Name:        "compress",
			Usage:       "compress the data",
			Destination: &compress,
		},
	}

	app.Action = func(c *cli.Context) error {
		mysqlsplit(c)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func mysqlsplit(c *cli.Context) {
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
		"# START\n",
		"SET AUTOCOMMIT = 0;\n",
	}
	postfix := []string{"# END\n"}
	if c.BoolT("add-check-off-prefix") {
		prefix = append(prefix,
			"SET FOREIGN_KEY_CHECKS = 0;\n",
			"SET UNIQUE_CHECKS = 0;\n",
		)
	}
	if c.BoolT("add-check-on-suffix") {
		postfix = append(postfix,
			"SET UNIQUE_CHECKS = 1;\n",
			"SET FOREIGN_KEY_CHECKS = 1;\n",
		)
	}
	postfix = append(postfix, "COMMIT;\n")

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
				if compress {
					tables[tableName] = FileWriter("out/"+tableName+".sql.gz", &wgEnd)
				} else {
					tables[tableName] = FileWriter("out/"+tableName+".sql", &wgEnd)
				}
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
		if err != nil {
			log.Errorf("file open error %s:%s", name, err)
		}
		if compress {
			gzw := gzip.NewWriter(file)
			for line := range ch {
				gzw.Write([]byte(*line))
			}
			gzw.Close()
		} else {
			for line := range ch {
				_, err := file.Write([]byte(*line))
				if err != nil {
					panic(fmt.Sprintf("error writing %s: %s", name, err))
				}
			}
		}
		log.Noticef("Closing %s", name)
		file.Close()

	}(name, ch)
	return ch
}
