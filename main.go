package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Configuration struct {
	port int
	dataFile string
}

var locked bool = false
var dictData map[string][]string = make(map[string][]string)
var dictMutex sync.Mutex

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func handleRequest(c net.Conn, conf Configuration) {
	log.Debug("Request from ", c.RemoteAddr())
	data, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		log.Error("Error handling request: ", err)
		return
	}
	tmp := strings.TrimSpace(string(data))
	log.Debug("Got: ", tmp)

	tmpN := strings.SplitN(tmp, " ", 2)
	log.Debug("tmpN: ", tmpN)

	queryType := strings.TrimSpace(tmpN[0])
	queryArgsRaw := strings.TrimSpace(tmpN[1])

	log.Debug("Query Type: `%s`\n", queryType)

	var key string
	var value string

	//parse key and value according to query type
	switch queryType {
	case "GET":
		regex := regexp.MustCompile(`<(?P<query>.{1,255})>`)
		//log.Println("submatches: ", regex.FindStringSubmatch(queryArgsRaw))
		//log.Println("subexpnames: ", regex.SubexpNames())
		matches := regex.FindStringSubmatch(queryArgsRaw)
		if len(matches) == 2{
			key = matches[1]
		}
		break
	case "PUT":
		regex := regexp.MustCompile(`<(?P<query>.{1,255})> <(?P<value>.{1,255})>`)
		//log.Println("submatches: ", regex.FindStringSubmatch(queryArgsRaw))
		//log.Println("subexpnames: ", regex.SubexpNames())
		matches := regex.FindStringSubmatch(queryArgsRaw)
		if len(matches) == 3{
			key = matches[1]
			value = matches[2]
		}
		break
	case "DELETE":
		regex := regexp.MustCompile(`<(?P<query>.{1,255})> <(?P<value>.{1,255})>`)
		//log.Println("submatches: ", regex.FindStringSubmatch(queryArgsRaw))
		//log.Println("subexpnames: ", regex.SubexpNames())
		matches := regex.FindStringSubmatch(queryArgsRaw)
		if len(matches) == 3{
			key = matches[1]
			value = matches[2]
		}
		break
	default:
		break
	}

	log.Debugf("key <%[1]T> `%[1]s`\n", key)
	log.Debugf("value <%[1]T> `%[1]s`\n", value)

	var found []string

	dictMutex.Lock()
	switch queryType {
	case "GET":
		found = dictData[key]
		break
	case "PUT":
		if ! contains(dictData[key], value) {
			dictData[key] = append(dictData[key], value)
		}
		found = dictData[key]
		break
	case "DELETE":
		if len(dictData[key]) == 1 {
			delete(dictData, key)
		} else if len(dictData[key]) > 1 {
			for idx, el := range dictData[key] {
				if el == value {
					dictData[key] = append(dictData[key][:idx], dictData[key][idx+1:]...)
				}
			}
		}
		found = dictData[key]
		break
	}
	dictMutex.Unlock()

	log.Debugf("found <%[1]T> `%[1]s`\n", found)

	var res string

	if len(found) > 0 {
		var tmp2 string =""
		for _, el := range found {
			tmp2 += fmt.Sprintf(" <%s>", el)
		}
		res = fmt.Sprintf("200%s\n", tmp2)
	} else {
		res = fmt.Sprintln("404")
	}

	log.Debugln("Sending back `", res, "`")
	c.Write([]byte(string(res)))
	c.Close()
}

func main() {
	log.Println("UltraVIM Dictionary server starting up...")

	cnf := Configuration{port: 27000, dataFile: "./data.dat"}

	log.Println("Gonna run with port ", cnf.port)

	if _, err := os.Stat(cnf.dataFile); err == nil {
		// path/to/whatever exists
		log.Println("Data file exists...")
		loadFromDataFile(cnf)
	} else if os.IsNotExist(err) {
		// path/to/whatever does *not* exist
		log.Println("Data file doesn't exist. Creating one now...")
		initializeDataFile(cnf)
	} else {
		// Schrodinger: file may or may not exist. See err for details.

		// Therefore, do *NOT* use !os.IsNotExist(err) to test for file existence
	}

	SetupCloseHandler(cnf)

	ticker := time.NewTicker(30 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <- ticker.C:
				// do stuff
				saveDataToFile(cnf)
			case <- quit:
				ticker.Stop()
				return
			}
		}
	}()
	l, err := net.Listen("tcp4", ":" + strconv.Itoa(cnf.port))
	if err != nil {
		log.Fatalln("Network error: ", err)
		return
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalln(err)
			return
		}
		go handleRequest(c, cnf)
	}
}

func loadFromDataFile(cnf Configuration) {
	log.Println("Loading data from file...")
	f, err := os.Open(cnf.dataFile)
	if err != nil {
		log.Fatal("Couldn't open data file.")
	}
	defer f.Close()

	decoder := gob.NewDecoder(f)
	if err := decoder.Decode(&dictData); err != nil {
		log.Fatal("Couldn't load dictData from file.")
	}
}

func saveDataToFile(cnf Configuration) {
	log.Println("Saving data to file...")
	f, err := os.Create(cnf.dataFile)
	if err != nil {
		log.Fatal("Couldn't create data file: ", err)
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(dictData); err != nil {
		log.Fatal("Couldn't write encoded dictData to file: ", err)
	}
}

func initializeDataFile(cnf Configuration) {
	log.Println("Initializing data file...")
	f, err := os.Create(cnf.dataFile)
	if err != nil {
		log.Fatal("Couldn't create data file: ", err)
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(dictData); err != nil {
		log.Fatal("Couldn't write encoded dictData to file: ", err)
	}
}

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our clean up procedure and exiting the program.
func SetupCloseHandler(cnf Configuration) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		saveDataToFile(cnf)
		os.Exit(0)
	}()
}

