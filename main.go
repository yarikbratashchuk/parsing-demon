package main

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/net/html"

	_ "github.com/lib/pq"
)

var (
	configMap = make(map[string]interface{})
	depthConf = make(map[string]interface{})
	Iterable  = struct {
		sync.Mutex
		Val int
	}{Val: 0}
	toParseChan = make(chan string, 3)
	toSaveChan  = make(chan []string, 5)
	wg          sync.WaitGroup
	splitFunc   = func(c rune) bool {
		return unicode.IsControl(c)
	}
	closeReq       = make(chan bool, 3)
	closeParseChan = make(chan bool, 3)
	reqDone        = make(chan bool, 3)
	parseDone      = make(chan bool, 3)
	db             *sql.DB
)

func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	config, err := os.Open("config.json")
	defer config.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = json.NewDecoder(config).Decode(&configMap)
	if err != nil {
		log.Fatal(err)
	}
	parseConf, err := os.Open("parseConfig.json")
	defer parseConf.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = json.NewDecoder(parseConf).Decode(&depthConf)
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	for i := 0; i < 3; i++ {
		go apiRequest(configMap, client)
	}
	go reqCloser()
	for i := 0; i < 3; i++ {
		go parseHtml()
	}
	go parseCloser()
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go saveParsed()
	}
	wg.Wait()
}

func saveParsed() {
	defer wg.Done()
	for val := range toSaveChan {
		go saveToDB(val)
	}
}

func saveToDB(data []string) {
	var code string
	row := db.QueryRow("SELECT code FROM autoParts WHERE code=$1", data[0])
	err := row.Scan(&code)
	if err != nil {
		log.Print(err)
		res, err := db.Exec("INSERT INTO autoParts (code,name,
	}
}

func parseHtml() {
	for ch := range toParseChan {
		z := html.NewTokenizer(strings.NewReader(ch))
		parseData(z)
	}
	parseDone <- true
	return
}

func parseData(z *html.Tokenizer) {
	var (
		depth    int
		inText   int
		textLoop int
		saved    bool
	)
	toSaveArr := make([]string, 0, 100)
	for n := 1; ; n++ {
		tt := z.Next()
		if !saved && len(toSaveArr)%8 == 0.0 && len(toSaveArr) != 0 {
			toSaveChan <- toSaveArr[len(toSaveArr)-8:]
			saved = true
		}
		switch tt {
		case html.ErrorToken:
			return
		case html.TextToken:
			if thatDepth(depth) {
				val := strings.TrimSpace(z.Token().Data)
				if val != "" {
					toSaveArr = append(toSaveArr, val)
					saved = false
				}
				inText = -1
			}
		case html.StartTagToken, html.EndTagToken:
			tn, _ := z.TagName()
			if notIgnore(string(tn)) {
				if tt == html.StartTagToken && !selfClosing(string(tn)) {
					depth++
					if thatDepth(depth) {
						inText = 1
						textLoop = n
					}
				} else if tt == html.EndTagToken {
					if inText == 1 && n == textLoop+1 {
						toSaveArr = append(toSaveArr, "EMPTY")
						saved = false
						inText = -1
						textLoop = 0
					}
					depth--
				}
			}
		}
	}
}

func notIgnore(token string) bool {
	for k, b := range depthConf {
		if k == "ignore" {
			for _, dv := range b.([]interface{}) {
				if dv.(string) == token {
					return false
				}
			}
		}
	}
	return true
}

func selfClosing(tagName string) bool {
	for k, b := range depthConf {
		if k == "selfClosing" {
			for _, dv := range b.([]interface{}) {
				if dv.(string) == tagName {
					return true
				}
			}
		}
	}
	return false
}

func thatDepth(a int) bool {
	for k, b := range depthConf {
		if k == "valDepths" {
			for _, dv := range b.([]interface{}) {
				if int(dv.(float64)) == a {
					return true
				}
			}
		}
	}
	return false
}

func apiRequest(configMap map[string]interface{}, client *http.Client) {
	for {
		select {
		case <-time.After(1 * time.Nanosecond):
			form := url.Values{}
			for _, value := range configMap["form"].([]interface{}) {
				field := strings.Split(value.(string), "=")
				if field[1] != "increment" {
					form.Add(field[0], field[1])
				} else {
					Iterable.Mutex.Lock()
					form.Add(field[0], strconv.Itoa(Iterable.Val))
					Iterable.Val++
					Iterable.Mutex.Unlock()
				}
			}
			req, err := http.NewRequest(configMap["method"].(string), configMap["api"].(string)+"?"+form.Encode(), nil)
			if err != nil {
				log.Fatal(err)
			}
			for _, h := range configMap["headers"].([]interface{}) {
				val := strings.Split(h.(string), ": ")
				req.Header.Add(val[0], val[1])
			}
			resp, err := client.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			rows := string(body)
			if err != nil {
				log.Fatal(err)
			}
			if len(strings.Fields(rows)) == 0 {
				closeReq <- true
			}
			rows = strings.Replace(rows, "\t", "", -1)
			rows = strings.Replace(rows, "\r", "", -1)
			rows = strings.Replace(rows, "\n", "", -1)
			toParseChan <- rows
		case <-reqDone:
			closeParseChan <- true
			return
		}
	}
}

func parseCloser() {
	allDone := 0
	for {
		select {
		case <-parseDone:
			allDone++
			if allDone == 3 {
				close(toSaveChan)
				return
			}
		}
	}
}

func reqCloser() {
	allDone := 0
	closed := false
	for {
		select {
		case <-closeReq:
			if !closed {
				close(reqDone)
				closed = true
			}
		case <-closeParseChan:
			allDone++
			if allDone == 3 {
				close(toParseChan)
				return
			}
		}
	}
}
