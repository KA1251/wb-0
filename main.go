package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

const (
	clientID   = "testclient"
	channel    = "pgnni"
	cache_file = "cache.json"
	natsURL    = "0.0.0.0:4222"
)

var (
	nc *nats.Conn

	cache_data []Cache
	nats_data  []Cache
)

type Cache struct {
	Order    Order    `json:"order"`
	Delivery Delivery `json:"delivery"`
	Payment  Payment  `json:"payment"`
	Items    []Items  `json:"items"`
}

type Order struct {
	Order_uid          string    `json:"order_uid"`
	Track_number       string    `json:"track_number"`
	Entry              string    `json:"enry"`
	Locale             string    `json:"locale"`
	Internal_signature string    `json:"internal_signature"`
	Customer_id        string    `json:"customer_id"`
	Delivery_servise   string    `json:"delivery_service"`
	Shardkey           string    `json:"shardkey"`
	Sm_id              int       `json:"sm_id"`
	Date_created       time.Time `json:"date_created"`
	Oof_shard          string    `json:"oof_shard"`
}
type Delivery struct {
	Name      string `json:"name"`
	Phone     string `json:"phone"`
	Zip       string `json:"zip"`
	City      string `json:"city"`
	Address   string `json:"address"`
	Region    string `json:"region"`
	Email     string `json:"email"`
	Order_uid string `json:"order_uid"`
}
type Payment struct {
	Transaction   string `json:"transaction"`
	Request_id    string `json:"request_id"`
	Currency      string `json:"currency"`
	Provider      string `json:"provider"`
	Amount        int    `json:"amount"`
	Payment_dt    int    `json:"payment_dt"`
	Bank          string `json:"bank"`
	Delivery_cost int    `json:"delivery_cost"`
	Goods_total   int    `json:"goods_total"`
	Custom_fee    int    `json:"custom_fee"`
}
type Full_data struct {
	Order           Order           `json:"order"`
	Delivery        Delivery        `json:"delivery"`
	Payment         Payment         `json:"payment"`
	Page_data_Items Page_data_Items `json:"page_data_items"`
	//Items Items
}

type Items struct {
	Chrt_id      int    `json:"chrt_id"`
	Track_number string `json:"track_number"`
	Price        int    `json:"price"`
	Rid          string `json:"rid"`
	Name         string `json:"name"`
	Sale         string `json:"sale"`
	Size         string `json:"size"`
	Total_prize  string `json:"total_prize"`
	Nm_id        int    `json:"nm_id"`
	Brand        string `json:"brand"`
	Status       int    `json:"status"`
	Order_uid    string `json:"order_uid"`
}
type Page_data_Items struct {
	Items []Items `json:"items"`
}

func GetUniqueElements(nats_data []Cache, bd_data []Cache) ([]Cache, error) {
	// Создаем мапу для хранения существующих элементов для проверки уникальности
	existingElements := make(map[string]bool)
	var uniq []Cache
	for i := 0; i < len(bd_data); i++ {
		existingElements[bd_data[i].Order.Order_uid] = true
	}

	// Проверка на уникальность каждого нового элемента

	for i := 0; i < len(nats_data); i++ {
		if existingElements[nats_data[i].Order.Order_uid] {

		} else {

			uniq = append(uniq, nats_data[i])
		}
	}

	return uniq, nil
}
func Recovery_from_bd() ([]Cache, error) {
	db, err := sql.Open("postgres", "user=kirill1  dbname=wbtechlevelzero password=1251 sslmode=disable")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	defer db.Close()

	var db_data_order []Order
	var db_data_delivery []Delivery
	var db_data_payment []Payment
	var db_data_items []Items

	rows, err := db.Query("SELECT order_uid,track_number,entry,locale,internal_signature,customer_id,delivery_service,shardkey,sm_id,date_created,oof_shard" +
		" FROM body ")

	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var data Order
		err := rows.Scan(&data.Order_uid, &data.Track_number, &data.Entry, &data.Locale, &data.Internal_signature, &data.Customer_id, &data.Delivery_servise, &data.Shardkey, &data.Sm_id, &data.Date_created, &data.Oof_shard)
		if err != nil {
			log.Fatal(err)
		}
		db_data_order = append(db_data_order, data)
	}
	rows, err = db.Query("SELECT name, phone, zips,city,address,region,email,order_uid " +
		"From delivery ")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var data Delivery
		err := rows.Scan(&data.Name, &data.Phone, &data.Zip, &data.City, &data.Address, &data.Region, &data.Email, &data.Order_uid)
		if err != nil {

			log.Fatal(err)
		}
		db_data_delivery = append(db_data_delivery, data)
	}
	rows, err = db.Query("SELECT transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee " +
		"FROM payment ")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var data Payment
		err := rows.Scan(&data.Transaction, &data.Request_id, &data.Currency, &data.Provider, &data.Amount, &data.Payment_dt, &data.Bank, &data.Delivery_cost, &data.Goods_total, &data.Custom_fee)
		if err != nil {
			log.Fatal(err)
		}
		db_data_payment = append(db_data_payment, data)
	}
	rows, err = db.Query("SELECT chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status,order_uid " +
		"FROM items ")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var db_data_item Items
		err := rows.Scan(&db_data_item.Chrt_id, &db_data_item.Track_number, &db_data_item.Price, &db_data_item.Rid, &db_data_item.Name, &db_data_item.Sale, &db_data_item.Size, &db_data_item.Total_prize, &db_data_item.Nm_id, &db_data_item.Brand, &db_data_item.Status, &db_data_item.Order_uid)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		db_data_items = append(db_data_items, db_data_item)
	}

	var fullDataCache []Cache

	var test Cache

	for i := 0; i < len(db_data_order); i++ {
		fullDataCache = append(fullDataCache, test)
		fullDataCache[i].Order = db_data_order[i]
		fullDataCache[i].Items = make([]Items, 0)

		for j := 0; j < len(db_data_delivery); j++ {
			if db_data_delivery[j].Order_uid == db_data_order[i].Order_uid {
				fullDataCache[i].Delivery = db_data_delivery[j]
				break
			}
		}
		for c := 0; c < len(db_data_payment); c++ {
			if db_data_payment[c].Transaction == db_data_order[i].Order_uid {
				fullDataCache[i].Payment = db_data_payment[c]
				break
			}
		}
		for d := 0; d < len(db_data_items); d++ {
			if db_data_items[d].Order_uid == db_data_order[i].Order_uid {
				fullDataCache[i].Items = append(fullDataCache[i].Items, db_data_items[d])

			}
		}
	}

	return fullDataCache, nil
}
func Get_data_from_cache(id string) Full_data {

	var result Full_data
	for _, data := range cache_data {

		if data.Order.Order_uid == id {
			result.Order = data.Order
			result.Delivery = data.Delivery
			result.Payment = data.Payment

		}
	}

	for i := 0; i < len(cache_data); i++ {
		for _, item := range cache_data[i].Items {
			if item.Order_uid == id {
				result.Page_data_Items.Items = append(result.Page_data_Items.Items, item)
			}
		}
	}

	return result

}

func Push_data_to_bd(db_data []Cache) {

	db, err := sql.Open("postgres", "user=kirill1  dbname=wbtechlevelzero password=1251 sslmode=disable") //дописать
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			log.Fatal(err)
		} else {
			tx.Commit()

		}

	}()

	for _, db_data := range db_data {
		_, err := tx.Exec("insert into body (track_number,entry,locale,internal_signature,"+
			"customer_id,delivery_service,shardkey,sm_id,"+
			"date_created,oof_shard,order_uid)"+
			"values 	($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);", db_data.Order.Track_number, db_data.Order.Entry, db_data.Order.Locale, db_data.Order.Internal_signature, db_data.Order.Customer_id, db_data.Order.Delivery_servise, db_data.Order.Shardkey, db_data.Order.Sm_id, db_data.Order.Date_created, &db_data.Order.Oof_shard, &db_data.Order.Order_uid)
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.Exec("insert into delivery (name,phone,zips,city,address,region, email,order_uid)"+
			"values($1,$2,$3,$4,$5,$6,$7,$8);", db_data.Delivery.Name, db_data.Delivery.Phone, db_data.Delivery.Zip, db_data.Delivery.City, db_data.Delivery.Address, db_data.Delivery.Region, db_data.Delivery.Email, db_data.Delivery.Order_uid)
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.Exec("insert into payment (transaction,request_id,currency,provider,amount,payment_dt,bank,delivery_cost,goods_total,custom_fee)"+
			"values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);", db_data.Payment.Transaction, db_data.Payment.Request_id, db_data.Payment.Currency, db_data.Payment.Provider, db_data.Payment.Amount, db_data.Payment.Payment_dt, db_data.Payment.Bank, db_data.Payment.Delivery_cost, db_data.Payment.Goods_total, db_data.Payment.Custom_fee)
		if err != nil {
			log.Fatal(err)
		}

	}

	for _, data := range db_data {
		for _, item := range data.Items {
			_, err = tx.Exec("insert into items(chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status,order_uid)"+
				"values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", item.Chrt_id, item.Track_number, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.Total_prize, item.Nm_id, item.Brand, item.Status, item.Order_uid)

		}
	}
	defer db.Close()
}

func Cache_file_create() {
	os.Create(cache_file)

}

func Load_data() {
	Cache_file_create()
}

func Save_data() {
	// Маршалим новые данные в JSON
	file_data, err := json.MarshalIndent(cache_data, "", " ")
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return
	}

	// Записываем новые данные в файл (перезаписываем существующий файл)
	err = os.WriteFile(cache_file, file_data, 0644)
	if err != nil {
		log.Printf("Error writing to file %s: %v", cache_file, err)
	}

}

func Subscribe_to_Nats() {
	var err error
	nc, err = nats.Connect(natsURL)
	if err != nil {

		log.Fatal(err)
	}

	_, err = nc.Subscribe(channel, func(msg *nats.Msg) {

		log.Println("данные записаны в кэш")
		err := json.Unmarshal(msg.Data, &nats_data)

		if err != nil {
			log.Println("Error unmarshaling JSON data:", err)
			log.Println("JSON data:", string(msg.Data))
			log.Fatal(err)

		}
		uniq, err := GetUniqueElements(nats_data, cache_data)

		Push_data_to_bd(uniq)

		cache_data = append(cache_data, uniq...)

	})
}
func main() {
	cache_data, _ = Recovery_from_bd()
	Subscribe_to_Nats()

	orderRouter := mux.NewRouter()
	orderRouter.HandleFunc("/main", Id_Handler)
	orderRouter.HandleFunc("/main/order", Id_process_handler3)

	http.ListenAndServe(":1251", orderRouter)

}

func Id_Handler(w http.ResponseWriter, r *http.Request) {
	tmp := template.Must(template.New("index").Parse(`
		<!DOCTYPE html>
		<html>
		<head>
			<title>Получение данных по ID</title>
		</head>
		<body>
			<h1>Получение данных по ID</h1>
			<form action="/main/order" method="post">
				<label>ID: <input type="text" name="id"></label>
				<input type="submit" value="Получить данные">
			</form>
		</body>
		</html>
		`))
	tmp.Execute(w, nil)
}

func Id_process_handler3(w http.ResponseWriter, r *http.Request) {
	id_str := r.FormValue("id")
	id_str = strings.ReplaceAll(id_str, " ", "")

	data := Get_data_from_cache(id_str)

	tmpl, err := template.ParseFiles("form.html")
	if err != nil {
		log.Fatal(err)
	}

	// Передача данных в шаблон и отображение на веб-странице

	tmpl.Execute(w, data)
}
