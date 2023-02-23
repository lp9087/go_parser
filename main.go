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

func getRemoteExtremistMaterials(httpClient *http.Client) map[string]string {
	page := getRequest(httpClient, fmt.Sprintf("%s/extremist-materials/", ExtremistMaterialBaseUrl))
	csvUrl := parseExtremistMaterialsPage(page)
	csvFile := getRequest(httpClient, csvUrl)
	remoteData := parseCSVFile(csvFile)
	return remoteData
}

func getDBExtremistMaterials(dsn string) map[string]int {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err == nil {
		fmt.Println("error")
	}
	var extremistMaterials []ExtremistMaterial

	db.Find(&extremistMaterials)

	result := make(map[string]int)

	for _, field := range extremistMaterials {
		result[field.Material] = field.ID
	}

	return result
}

func insertDBExtremistMaterials(dsn string, toCreate []ExtremistMaterial) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err == nil {
		fmt.Println("error")
	}
	db.CreateInBatches(&toCreate, len(toCreate))

}

func deleteDBExtremistMaterials(dsn string, toDelete []int) {

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err == nil {
		fmt.Println("error")
	}
	var extremistMaterial ExtremistMaterial
	db.Delete(&extremistMaterial, toDelete)

}

func getRequest(c *http.Client, url string) []byte {

	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("User-Agent", `MY_AGENT`)
	resp, err := c.Do(req)
	if err != nil {
		fmt.Println("Err is", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Err is", err)
	}
	return body
}

func parseExtremistMaterialsPage(page []byte) string {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		log.Fatal("Error loading HTTP response body. ", err)
	}

	var urlPart string

	doc.Find("a").EachWithBreak(func(i int, s *goquery.Selection) bool {
		href, exists := s.Attr("href")
		if exists && strings.Contains(href, ".csv") {
			// Process href here
			urlPart = href
			return false
		}
		return true
	})
	csvUrl := fmt.Sprintf("%s%s", ExtremistMaterialBaseUrl, urlPart)
	return csvUrl
}

func parseCSVFile(file []byte) map[string]string {
	// Read the response body into a byte slice.
	data, err := io.ReadAll(bytes.NewReader(file))
	if err != nil {
		log.Fatal(err)
	}

	// Decode the byte slice into a UTF-8 string.
	decoder := charmap.Windows1251.NewDecoder()
	utf8Str, err := decoder.String(string(data))
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(strings.NewReader(utf8Str))
	reader.Comma = ';'
	reader.LazyQuotes = true

	//records, err := reader.ReadAll()

	results := make(map[string]string)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
			//return nil, err
		}

		// add record to result set
		record[1] = strings.ReplaceAll(record[1], "\"", "'")
		results[record[1]] = record[2]
	}
	return results
}

func compareData(dbData map[string]int, remoteData map[string]string) ([]ExtremistMaterial, []int) {
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
	dbData := getDBExtremistMaterials(dbAddress)
	remote := getRemoteExtremistMaterials(client)

	toCreate, toDelete := compareData(dbData, remote)

	if len(toCreate) > 0 {
		insertDBExtremistMaterials(dbAddress, toCreate)
	}
	if len(toDelete) > 0 {
		deleteDBExtremistMaterials(dbAddress, toDelete)
	}
}

func main() {
	client := &http.Client{}
	dsn := "host=localhost user=rkn-dashboard-admin-user password=rkn-dashboard-admin-pass dbname=rkn-dashboard-admin-db port=5432"
	updateExtremistMaterials(client, dsn)
}
