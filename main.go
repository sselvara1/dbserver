package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// We must never define passwords in plain text for security reasons but for
// the sake of this program, we will define them in plain text.
const (
	username = "root"
	password = "password"
	hostname = "127.0.0.1:3306"
)

type payload struct {
	Name     string
	Engine   string
	Size     string
	Replicas int64
}

type response struct {
	uuid string
}

type metaData struct {
	Name     string
	Engine   string
	Size     string
	Replicas int64
	uuid     string
}

// metadata for list of DBs
var metaDataList = []metaData{}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func findRecordsByDBName(name string) int {
	for p, v := range metaDataList {
		// check equal under Unicode case-folding
		if strings.EqualFold(v.Name, name) {
			return p
		}
	}
	return -1
}

func createDB(w http.ResponseWriter, req *http.Request) {
	log.Printf("got /createDB request\n")

	decoder := json.NewDecoder(req.Body)
	var t payload
	err := decoder.Decode(&t)
	if err != nil {
		panic(err)
	}

	if findRecordsByDBName(t.Name) != -1 {
		fmt.Println("DB is present" + t.Name)
		return
	}

	// an empty dbName will be passed to the dsn function.
	// open and return a connection to the database
	db, err := sql.Open(t.Engine, dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return
	}

	// After establishing a connection to DB, next step is to create DB.
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)

	// cancelfunc is only needed to cancel the context before it times out.
	// There is no use of it here, hence we just defer the cancelfunc call.
	defer cancelfunc()

	// Being a responsible developer, we pass a context with a timeout of
	// 5 seconds to ensure that program doesn’t get stuck when creating DB
	// in case there is any network error or any other error in the DB
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+t.Name)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected: %d\n", no)
	db.Close()

	// In the above line, we close the existing connection and open a new
	// connection to the DB. This time we specify the DB name when opening a
	// connection to the database.
	db, err = sql.Open(t.Engine, dsn(t.Name))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return
	}
	defer db.Close()

	// There are few important connection pool options to be set to ensure that
	// network partitions and other runtime errors that may occur with our DB
	// connections are handled properly.

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return
	}
	log.Printf("Connected to DB %s successfully\n", t.Name)

	// get uuid for the DB created
	uuid := 0
	queryGetUUID := db.QueryRow(`SELECT UUID_SHORT()`)
	err = queryGetUUID.Scan(
		&uuid,
	)
	if err != nil {
		log.Printf("Errors %s getting UUID", err)
		return
	}
	log.Printf("UUID %d\n", uuid)

	var d response
	d.uuid = fmt.Sprintf("%v", uuid)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, "UUID of DB created: ")
	json.NewEncoder(w).Encode(d.uuid)

	// update metadata
	m := metaData{t.Name, t.Engine, t.Size, t.Replicas, d.uuid}
	metaDataList = append(metaDataList, m)
	log.Println(metaDataList)
}

func deleteDB(w http.ResponseWriter, r *http.Request) {
	log.Printf("got /deleteDB request\n")

	dbName := r.URL.Query().Get("dbName")
	log.Println("dbName =>", dbName)

	p := findRecordsByDBName(dbName)
	if p == -1 {
		log.Println("DB is not present" + dbName)
		return
	}
	log.Printf("DB is present at pos: %v", p)

	db, err := sql.Open("mysql", dsn(dbName))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return
	}
	defer db.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	res, err := db.ExecContext(ctx, "DROP DATABASE "+dbName)
	if err != nil {
		log.Println(err)
		return
	}

	rowCount, err := res.RowsAffected()
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("rows affected: %d\n", rowCount)

	// update metadata
	metaDataList = append(metaDataList[:p], metaDataList[p+1:]...)
	log.Println(metaDataList)
}

func getMetadata(w http.ResponseWriter, r *http.Request) {
	log.Printf("got /getMetadata request\n")
	log.Println(metaDataList)
}

func main() {
	http.HandleFunc("/createDB", createDB)
	http.HandleFunc("/deleteDB", deleteDB)
	http.HandleFunc("/getMetadata", getMetadata)

	err := http.ListenAndServe(":3333", nil)

	if errors.Is(err, http.ErrServerClosed) {
		log.Printf("server closed\n")
	} else if err != nil {
		log.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
