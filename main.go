// Copyright 2016 The Parsing-demon Authors.
// Use of this source code is free

// HTML table response reading, parsing and updating in db

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
	configMap    = make(map[string]interface{})
	parseConfMap = make(map[string]interface{})
	Iterable     = struct {
		sync.Mutex
		Val int
	}{Val: 0}
	partsCategory int
	wg            sync.WaitGroup
	splitFunc     = func(c rune) bool {
		return unicode.IsControl(c)
	}
	closeReq       = make(chan bool, 3)
	closeParseChan = make(chan bool, 3)
	parseDone      = make(chan bool, 3)
	db             *sql.DB
)

func main() {
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
	err = json.NewDecoder(parseConf).Decode(&parseConfMap)
	if err != nil {
		log.Fatal(err)
	}
	db, err = sql.Open("postgres", configMap["dbURL"].(string))
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}

	for {
		toParseChan := make(chan string, 3)
		toSaveChan := make(chan []interface{}, 15)
		reqDone := make(chan bool, 3)
		for i := 0; i < 3; i++ {
			go apiRequest(configMap, client, toParseChan, reqDone)
		}
		go reqCloser(toParseChan, reqDone)
		for i := 0; i < 3; i++ {
			go parseHtml(toParseChan, toSaveChan)
		}
		go parseCloser(toSaveChan)
		wg.Add(3)
		for i := 0; i < 3; i++ {
			go saveParsed(toSaveChan)
		}
		wg.Wait()
		sleepPeriod := parseConfMap["updateDBPeriod"].(float64)
		log.Printf("Sleeping for %v minute(s)", sleepPeriod)
		time.Sleep(time.Duration(sleepPeriod) * time.Minute)
	}
}

// saveParsed receives data arrays and for each creates saveToDB goroutine
func saveParsed(toSaveChan <-chan []interface{}) {
	defer wg.Done()
	for val := range toSaveChan {
		saveToDB(val)
	}
}

// parseHtml receives html table string and parses it
func parseHtml(toParseChan <-chan string, toSaveChan chan []interface{}) {
	for ch := range toParseChan {
		z := html.NewTokenizer(strings.NewReader(ch))
		parseData(z, toSaveChan)
	}
	parseDone <- true
	return
}

// parseData walks html tree and parses values that are at the
// desired depth and suits all criteria (that can be found in parseConf.json)
func parseData(z *html.Tokenizer, toSaveChan chan<- []interface{}) {
	var (
		depth    int
		inText   int
		textLoop int
		saved    bool
	)
	toSaveArr := make([]interface{}, 0, 100)
	cellsNum := int(parseConfMap["cellsInTableRow"].(float64))
	for n := 1; ; n++ {
		tt := z.Next()
		if !saved && len(toSaveArr)%cellsNum == 0.0 && len(toSaveArr) != 0 {
			toSaveChan <- toSaveArr[len(toSaveArr)-cellsNum:]
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
			if !ignore(string(tn)) {
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

// ignore checks if current tag shoud be ignored while parsing data
func ignore(token string) bool {
	for k, b := range parseConfMap {
		if k == "ignore" {
			for _, dv := range b.([]interface{}) {
				if dv.(string) == token {
					return true
				}
			}
		}
	}
	return false
}

// selfClosing checks if the current opening tag should
// not be considered in depth tracing
func selfClosing(tagName string) bool {
	for k, b := range parseConfMap {
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

// thatDepth check whether tokenizer is now at
// the desired depth in html table (where value metters to us)
func thatDepth(a int) bool {
	for k, b := range parseConfMap {
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

// saveToDB configures values in array to match
// desired in DB, than it checks whether table row is in the DB.
// Than it performs INSERT of UPDATE transaction
func saveToDB(data []interface{}) {
	var (
		code      string
		less_than bool
	)
	fData := make([]interface{}, 0, 11)
	if strings.Contains(data[3].(string), ">") {
		less_than = true
		data[3], _ = strconv.Atoi(data[3].(string)[1:])
	} else {
		data[3], _ = strconv.Atoi(data[3].(string))
	}
	data[4], _ = strconv.Atoi(data[4].(string))
	data[5], _ = strconv.ParseFloat(data[5].(string), 32)
	data[6], _ = strconv.ParseFloat(data[6].(string), 32)
	for i, el := range data {
		if el == "EMPTY" {
			data[i] = ""
		}
	}
	fData = append(fData, data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], partsCategory, less_than)
	row := db.QueryRow("SELECT code FROM \"autoParts_autoparts\" WHERE code=$1", data[0])
	err := row.Scan(&code)
	if err != nil {
		log.Print(err)
		err := db.QueryRow("INSERT INTO \"autoParts_autoparts\" (code, part_name, brand, quantity, reserved, "+
			"price, price_with_discount, description, category, less_than) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", fData...)
		if err != nil {
			log.Println(err)
		}
		return
	}
	fData = append(fData[1:], code)
	_, err = db.Exec("UPDATE \"autoParts_autoparts\" SET part_name=$1, brand=$2, quantity=$3, reserved=$4, "+
		"price=$5, price_with_discount=$6, description=$7, category=$8, less_than=$9 WHERE code=$10", fData...)
	if err != nil {
		log.Println(err)
	}
	return
}

// apiRequest makes request to the html table endpoint, formats response string
// and sends via toParseChan to parsing goroutines
func apiRequest(configMap map[string]interface{}, client *http.Client, toParseChan chan<- string, reqDone <-chan bool) {
	for {
		select {
		case <-time.After(1 * time.Nanosecond):
			form := url.Values{}
			for _, value := range configMap["form"].([]interface{}) {
				field := strings.Split(value.(string), "=")
				if field[1] != "increment" {
					form.Add(field[0], field[1])
					if field[0] == "category" {
						partsCategory, _ = strconv.Atoi(field[1])
					}
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

// parseCloser closes toSaveChan when pasing is done
func parseCloser(toSaveChan chan []interface{}) {
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

// reqCloser stops all apiRequest goroutines and than closes toParseChan
func reqCloser(toParseChan chan string, reqDone chan bool) {
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
