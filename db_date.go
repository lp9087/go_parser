package main

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"time"
)

type ExtremistMaterial struct {
	ID            int
	Material      string
	InclusionDate *time.Time
}

func (ExtremistMaterial) TableName() string {
	return "extremist_material"
}

func GetDBExtremistMaterials(dsn string, channel chan map[string]int) {
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

func InsertDBExtremistMaterials(dsn string, toCreate []ExtremistMaterial) {
	log.Println("_____Запись в БД_____")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к БД при попытке записи. Текст ошибки: %s", err)
	}
	db.CreateInBatches(&toCreate, len(toCreate))
	log.Println("_____Окончание записи в БД_____")

}

func DeleteDBExtremistMaterials(dsn string, toDelete []int) {
	log.Println("_____Удаление из БД_____")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к БД при попытке удаления. Текст ошибки: %s", err)
	}
	var extremistMaterial ExtremistMaterial
	db.Delete(&extremistMaterial, toDelete)
	log.Println("_____Окончание удаления из БД_____")
}
