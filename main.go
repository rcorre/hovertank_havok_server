package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

type DB interface {
	Init() error
	GetRecords() ([]record, error)
	PutRecord(record) error
}

type db struct {
	*sql.DB
}

func (d *db) Init() error {
	_, err := d.Exec(
		"CREATE TABLE IF NOT EXISTS records(" +
			"name varchar NOT NULL," +
			"score integer NOT NULL" +
			")",
	)
	return err
}

func (d *db) GetRecords() ([]record, error) {
	rows, err := d.Query(
		"SELECT name, score FROM records ORDER BY score desc limit 10",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []record{}
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.Name, &rec.Score); err != nil {
			log.Printf("Bad record: %v", err)
		} else {
			res = append(res, rec)
		}
	}
	return res, rows.Err()
}

func (d *db) PutRecord(val record) error {
	stmt, err := d.Prepare(
		"INSERT INTO records(name, score) VALUES($1, $2)",
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(val.Name, val.Score)
	return err
}

type v1API struct {
	db DB
}

type record struct {
	Name  string
	Score int
}

func unmarshal(r io.Reader, out interface{}) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return fmt.Errorf("Failed to unmarshal %q: %v", b, err)
	}
	return nil
}

func (v1 *v1API) getRecords(w http.ResponseWriter, r *http.Request) {
	records, err := v1.db.GetRecords()
	if err != nil {
		log.Printf("Failed to get records: %v", err)
		http.Error(w, "Failed to get records", http.StatusInternalServerError)
		return
	}

	resp, err := json.Marshal(records)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(resp); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
	log.Println("GET records ok")
}

func (v1 *v1API) postRecord(w http.ResponseWriter, r *http.Request) {
	var entry record
	if err := unmarshal(r.Body, &entry); err != nil {
		log.Printf("Failed to parse POST body: %v", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if entry.Name == "" || entry.Score <= 0 {
		log.Println("Missing time or level")
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	if err := v1.db.PutRecord(entry); err != nil {
		log.Printf("Failed to store record: %v", err)
		http.Error(w, "Failed to store record", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	log.Println("POST record ok", entry.Name, entry.Score)
}

func newMux(db DB) *http.ServeMux {
	mux := http.NewServeMux()
	v1 := &v1API{db: db}

	mux.HandleFunc("/v1/records", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			v1.getRecords(w, r)
		} else if r.Method == http.MethodPost {
			v1.postRecord(w, r)
		} else {
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	})

	return mux
}

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		port = "8080"
	}

	pg, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	log.Println("Connected to DB")

	d := &db{pg}
	if err := d.Init(); err != nil {
		panic(err)
	}
	log.Println("DB Initialized")

	server := &http.Server{
		Handler: newMux(&db{pg}),
		Addr:    ":" + port,
	}
	log.Println("Listening on", port)
	log.Fatal(server.ListenAndServe())
}
