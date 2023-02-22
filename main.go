package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/jackc/pgx"
	"golang.org/x/text/encoding/charmap"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const ExtremistMaterialBaseUrl = "https://minjust.gov.ru"

type DBData struct {
	Id       int
	Material string
}

type ToCreate struct {
	Material      string
	InclusionDate string
}

func getRemoteExtremistMaterials(httpClient *http.Client) map[string]string {
	page := getRequest(httpClient, fmt.Sprintf("%s/extremist-materials/", ExtremistMaterialBaseUrl))
	csvUrl := parseExtremistMaterialsPage(page)
	csvFile := getRequest(httpClient, csvUrl)
	remoteData := parseCSVFile(csvFile)
	return remoteData
}

func getDBExtremistMaterials(dbConfig *pgx.ConnConfig) map[string]int {

	conn, err := pgx.Connect(*dbConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	raws, erro := conn.Query("SELECT id, material from extremist_material")
	if erro != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	materials := make(map[string]int)

	for raws.Next() {
		var r DBData
		err := raws.Scan(&r.Id, &r.Material)
		if err != nil {
			log.Fatal(err)
		}
		materials[r.Material] = r.Id
	}
	if err := raws.Err(); err != nil {
		log.Fatal(err)
	}

	return materials
}

func insertDBExtremistMaterials(dbConfig *pgx.ConnConfig, toCreate []ToCreate) any {

	conn, err := pgx.Connect(*dbConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	defer conn.Close()

	copyFromTable := pgx.Identifier{"extremist_material"}
	copyFromColumns := []string{"material", "inclusion_date"}

	copyFromRows := make([][]interface{}, len(toCreate))

	// Наполняем срез значениями
	for i, item := range toCreate {

		var row []interface{}
		dt := "2020.06.25"

		if item.InclusionDate == "" {
			row = []interface{}{item.Material, nil}
		} else {
			date, _ := time.Parse(dt, item.InclusionDate)
			row = []interface{}{item.Material, date}
		}

		copyFromRows[i] = row
	}

	copyCount, erro := conn.CopyFrom(
		copyFromTable,
		copyFromColumns,
		pgx.CopyFromRows(copyFromRows),
	)
	if erro != nil {
		fmt.Println("error")
	}
	return copyCount

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

func compareData(dbData map[string]int, remoteData map[string]string) ([]ToCreate, []int) {
	var toCreate []ToCreate
	var toDelete []int

	for material, inclusionDate := range remoteData {
		_, ok := dbData[material]
		if ok == false {
			toCreate = append(toCreate, ToCreate{material, inclusionDate})
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

func main() {
	client := &http.Client{}
	config := &pgx.ConnConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "rkn-dashboard-admin-user",
		Password: "rkn-dashboard-admin-pass",
		Database: "rkn-dashboard-admin-db",
	}
	dbData := getDBExtremistMaterials(config)
	remote := getRemoteExtremistMaterials(client)

	toCreate, toDelete := compareData(dbData, remote)

	if len(toCreate) > 0 {
		insertDBExtremistMaterials(config, toCreate)
	}

	println(toCreate, toDelete)
}
