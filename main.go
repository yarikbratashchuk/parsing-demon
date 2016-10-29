package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/html"

	_ "github.com/lib/pq"
)

var (
	Iterable = struct {
		sync.Mutex
		Val int
	}{Val: 0}
	toParseChan = make(chan io.Reader, 3)
	toSaveChan  = make(chan []string, 3)
	wg          sync.WaitGroup
	f           func(*html.Node)
)

func main() {
	configMap := make(map[string]interface{})
	//db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	//if err != nil {
	//	log.Fatal(err)
	//	}
	config, err := os.Open("config.json")
	defer config.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = json.NewDecoder(config).Decode(&configMap)
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	for i := 0; i < 3; i++ {
		go apiRequest(configMap, client)
	}
	for i := 0; i < 3; i++ {
		go parseHtml()
	}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go saveParsed()
	}
	wg.Wait()
}

func saveParsed() {
	for arr := range toSaveChan {
		for i, el := range arr {
		}
	}
}

func parseHtml() {
	defer wg.Done()
	f = func(n *html.Node) {
		if data := strings.TrimSpace(n.Data); n.Type == html.TextNode && len(data) != 0 {
			parsedRow = append(parsedRow, data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	for ch := range toParseChan {
		parsedRow := make([]string, 0, 28)
		doc, err := html.Parse(<-ch)
		if err != nil {
			log.Fatal(err)
		}
		f(doc)
		toSaveChan <- parsedRow
	}
}

func apiRequest(configMap map[string]interface{}, client *http.Client) {
	for {
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
		if err != nil {
			log.Fatal(err)
		}
		if len(strings.Fields(string(body))) == 0 {
			defer close(toParseChan)
			return
		}
		toParseChan <- resp.Body
	}
	return
}
