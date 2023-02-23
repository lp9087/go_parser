package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"golang.org/x/text/encoding/charmap"
	"io"
	"log"
	"net/http"
	"strings"
)

const ExtremistMaterialBaseUrl = "https://minjust.gov.ru"

func GetRemoteExtremistMaterials(httpClient *http.Client, channel chan map[string]string) {
	log.Println("_____Начало получения данных со стороннего источника_____")
	page := GetRequest(httpClient, fmt.Sprintf("%s/extremist-materials/", ExtremistMaterialBaseUrl))
	csvUrl := ParseExtremistMaterialsPage(page)
	csvFile := GetRequest(httpClient, csvUrl)
	remoteData := ParseCSVFile(csvFile)
	channel <- remoteData
}

func GetRequest(c *http.Client, url string) []byte {
	log.Printf("_____Запрос на сервис %s _____", url)
	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		log.Fatalf("Ошибка формирования запроса на url, %s. Текст ошибки: %s", url, reqErr)
	}
	req.Header.Add("User-Agent", `MY_AGENT`)
	resp, err := c.Do(req)
	if err != nil {
		log.Fatalf("Ошибка выполнения запроса на url, %s. Текст ошибки: %s", url, err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		log.Fatalf("Ошибка обработки ответа с url, %s. Текст ошибки: %s", url, readErr)
	}
	return body
}

func ParseExtremistMaterialsPage(page []byte) string {
	log.Println("_____Парсинг xml страницы_____")
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		log.Fatalf("Ошибка формирования xml из HTML страницы. Текст ошибки %s", err)
	}

	var urlPart string

	doc.Find("a").EachWithBreak(func(i int, s *goquery.Selection) bool {
		href, exists := s.Attr("href")
		if exists && strings.Contains(href, ".csv") {
			urlPart = href
			return false
		}
		return true
	})
	csvUrl := fmt.Sprintf("%s%s", ExtremistMaterialBaseUrl, urlPart)
	return csvUrl
}

func ParseCSVFile(file []byte) map[string]string {
	log.Println("_____Парсинг csv файла_____")
	data, err := io.ReadAll(bytes.NewReader(file))
	if err != nil {
		log.Fatalf("Ошибка формирования чтения XML. Текст ошибки %s", err)
	}

	decoder := charmap.Windows1251.NewDecoder()
	utf8Str, err := decoder.String(string(data))
	if err != nil {
		log.Fatalf("Ошибка кодировки XML. Текст ошибки %s", err)
	}

	reader := csv.NewReader(strings.NewReader(utf8Str))
	reader.Comma = ';'
	reader.LazyQuotes = true

	results := make(map[string]string)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		record[1] = strings.ReplaceAll(record[1], "\"", "'")
		results[record[1]] = record[2]
	}
	return results
}
