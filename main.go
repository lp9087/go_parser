package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"time"
)

func UpdateExtremistMaterials(client *http.Client, dbAddress string) {
	log.Println("_____Получение данных из источников_____")

	dbMaterials := make(chan map[string]int)
	remoteMaterials := make(chan map[string]string)

	go GetDBExtremistMaterials(dbAddress, dbMaterials)
	go GetRemoteExtremistMaterials(client, remoteMaterials)

	dbData := <-dbMaterials
	remote := <-remoteMaterials

	toCreate, toDelete := CompareData(dbData, remote)

	if len(toCreate) > 0 {
		go InsertDBExtremistMaterials(dbAddress, toCreate)
	}
	if len(toDelete) > 0 {
		go DeleteDBExtremistMaterials(dbAddress, toDelete)
	}
}

func CompareData(dbData map[string]int, remoteData map[string]string) ([]ExtremistMaterial, []int) {
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

func WaitSomeHours(hours int) {
	log.Printf("_____Ожидание %v часов_____", hours)
	time.Sleep(time.Duration(hours) * time.Hour)
	log.Printf("_____Окончание ожидания %v часов_____", hours)
}

func main() {
	log.Println("_____Запуск приложения_____")
	client := &http.Client{}

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Ошибка сбора локальных переменных с .env файла. Текст ошибки: %s", err)
	}

	Host := os.Getenv("DB_HOST")
	Port := os.Getenv("DB_PORT")
	User := os.Getenv("DB_USER")
	Password := os.Getenv("DB_PASSWORD")
	Name := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s", Host, User, Password, Name, Port)
	for {
		UpdateExtremistMaterials(client, dsn)
		WaitSomeHours(6)
	}
}
