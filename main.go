package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"golang.org/x/text/encoding/charmap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const ExtremistMaterialBaseUrl = "https://minjust.gov.ru"

type ExtremistMaterial struct {
	ID            int
	Material      string
	InclusionDate *time.Time
}

func (ExtremistMaterial) TableName() string {
	return "extremist_material"
}

func getRemoteExtremistMaterials(httpClient *http.Client, channel chan map[string]string) {
	log.Println("_____Начало получения данных со стороннего источника_____")
	page := getRequest(httpClient, fmt.Sprintf("%s/extremist-materials/", ExtremistMaterialBaseUrl))
	csvUrl := parseExtremistMaterialsPage(page)
	csvFile := getRequest(httpClient, csvUrl)
	remoteData := parseCSVFile(csvFile)
	channel <- remoteData
}

func getDBExtremistMaterials(dsn string, channel chan map[string]int) {
	log.Println("_____Подключаемся к БД_____")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к БД. Текст ошибки: %s", err)
	}
	var extremistMaterials []ExtremistMaterial

	log.Println("_____Поиск записей в БД_____")

	db.Find(&extremistMaterials)

	result := make(map[string]int)

	for _, field := range extremistMaterials {
		result[field.Material] = field.ID
	}
	channel <- result
}

func insertDBExtremistMaterials(dsn string, toCreate []ExtremistMaterial) {
	log.Println("_____Запись в БД_____")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к БД при попытке записи. Текст ошибки: %s", err)
	}
	db.CreateInBatches(&toCreate, len(toCreate))
	log.Println("_____Окончание записи в БД_____")

}

func deleteDBExtremistMaterials(dsn string, toDelete []int) {
	log.Println("_____Удаление из БД_____")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к БД при попытке удаления. Текст ошибки: %s", err)
	}
	var extremistMaterial ExtremistMaterial
	db.Delete(&extremistMaterial, toDelete)
	log.Println("_____Окончание удаления из БД_____")
}

func getRequest(c *http.Client, url string) []byte {
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

func parseExtremistMaterialsPage(page []byte) string {
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

func parseCSVFile(file []byte) map[string]string {
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

func compareData(dbData map[string]int, remoteData map[string]string) ([]ExtremistMaterial, []int) {
	log.Println("_____Сравнение данных из БД и стороннего ресурса_____")
	var toCreate []ExtremistMaterial
	var toDelete []int

	for material, inclusionDate := range remoteData {
		_, ok := dbData[material]
		if ok == false {

			var row ExtremistMaterial

			layout := "02.01.2006"

			if inclusionDate == "" {
				row = ExtremistMaterial{Material: material}
			} else {
				date, _ := time.Parse(layout, inclusionDate)
				row = ExtremistMaterial{Material: material, InclusionDate: &date}
			}

			toCreate = append(toCreate, row)
		}
	}

	for material, id := range dbData {
		_, ok := remoteData[material]
		if ok == false {
			toDelete = append(toDelete, id)
		}
	}
	return toCreate, toDelete
}

func updateExtremistMaterials(client *http.Client, dbAddress string) {
	log.Println("_____Получение данных из источников_____")

	dbMaterials := make(chan map[string]int)
	remoteMaterials := make(chan map[string]string)

	go getDBExtremistMaterials(dbAddress, dbMaterials)
	go getRemoteExtremistMaterials(client, remoteMaterials)

	dbData := <-dbMaterials
	remote := <-remoteMaterials

	toCreate, toDelete := compareData(dbData, remote)

	if len(toCreate) > 0 {
		go insertDBExtremistMaterials(dbAddress, toCreate)
	}
	if len(toDelete) > 0 {
		go deleteDBExtremistMaterials(dbAddress, toDelete)
	}
}

func waitSomeHours(hours int) {
	log.Printf("_____Ожидание %v часов_____", hours)
	time.Sleep(time.Duration(hours) * time.Hour)
	log.Printf("_____Окончание ожидания %v часов_____", hours)
}

func main() {
	log.Println("_____Запуск приложения_____")
	client := &http.Client{}
	dsn := "host=localhost user=rkn-dashboard-admin-user password=rkn-dashboard-admin-pass dbname=rkn-dashboard-admin-db port=5432"
	for {
		updateExtremistMaterials(client, dsn)
		waitSomeHours(6)
	}
}
