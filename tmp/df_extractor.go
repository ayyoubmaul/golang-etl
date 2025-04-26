// package main

// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"log"
// 	"sync"

// 	"github.com/go-gota/gota/dataframe"
// 	"github.com/go-gota/gota/series"
// 	_ "github.com/go-sql-driver/mysql"
// 	jsoniter "github.com/json-iterator/go"
// 	"github.com/xitongsys/parquet-go-source/local"
// 	"github.com/xitongsys/parquet-go/writer"
// )

// const (
// 	batchSize = 1000 // Number of records per batch
// )

// var dbConnections = map[string]string{
// 	"db_1": "root:root@tcp(127.0.0.1:3307)/jerry",
// 	"db_2": "root:root@tcp(127.0.0.1:3308)/pikachu",
// }

// // Row type that is used to store data from database
// type Row map[string]interface{}

// // Connect to each database and return the connection
// func connectToDB(dsn string) (*sql.DB, error) {
// 	db, err := sql.Open("mysql", dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("could not open db connection: %v", err)
// 	}
// 	if err := db.Ping(); err != nil {
// 		return nil, fmt.Errorf("could not ping db: %v", err)
// 	}
// 	return db, nil
// }

// // Fetch data from the database in parallel using goroutines and send it to a buffered channel
// func fetchDataFromShard(ctx context.Context, dbName, query string, dataChan chan<- Row, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	dsn := dbConnections[dbName]
// 	db, err := connectToDB(dsn)
// 	if err != nil {
// 		log.Printf("Error connecting to database %s: %v\n", dbName, err)
// 		return
// 	}
// 	defer db.Close()

// 	rows, err := db.QueryContext(ctx, query)
// 	if err != nil {
// 		log.Printf("Error executing query on database %s: %v\n", dbName, err)
// 		return
// 	}
// 	defer rows.Close()

// 	columns, err := rows.Columns()
// 	if err != nil {
// 		log.Println("Error getting columns:", err)
// 		return
// 	}

// 	for rows.Next() {
// 		values := make([]interface{}, len(columns))
// 		valuePtrs := make([]interface{}, len(columns))
// 		for i := range values {
// 			valuePtrs[i] = &values[i]
// 		}

// 		if err := rows.Scan(valuePtrs...); err != nil {
// 			log.Println("Error scanning row:", err)
// 			continue
// 		}

// 		row := make(Row)
// 		for i, col := range columns {
// 			if b, ok := values[i].([]byte); ok {
// 				row[col] = string(b) // Convert byte slice to string
// 			} else {
// 				row[col] = values[i]
// 			}
// 		}

// 		dataChan <- row
// 	}
// }

// // Convert rows to DataFrame and write to Parquet
// func writeParquet(ctx context.Context, outputFile string, dataChan <-chan Row, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	var rows []map[string]interface{}
// 	for row := range dataChan {
// 		rows = append(rows, row)
// 		if len(rows) >= batchSize {
// 			writeBatchToParquet(outputFile, rows)
// 			rows = nil // Clear rows after writing
// 		}
// 	}

// 	// Write any remaining rows to Parquet
// 	if len(rows) > 0 {
// 		writeBatchToParquet(outputFile, rows)
// 	}
// }

// // Write batch to Parquet file
// func writeBatchToParquet(outputFile string, rows []map[string]interface{}) {
// 	var columnNames []string
// 	for key := range rows[0] {
// 		columnNames = append(columnNames, key)
// 	}

// 	var columns []series.Series
// 	for _, col := range columnNames {
// 		var colData []interface{}
// 		for _, row := range rows {
// 			colData = append(colData, row[col])
// 		}
// 		columns = append(columns, series.New(colData, series.String, col))
// 	}

// 	df := dataframe.New(columns...)

// 	writeDataFrameToParquet(outputFile, df)
// }

// // Write DataFrame to Parquet file
// func writeDataFrameToParquet(outputFile string, df dataframe.DataFrame) {
// 	// Prepare parquet file and writer
// 	f, err := local.NewLocalFileWriter(outputFile)
// 	if err != nil {
// 		log.Println("Error creating file:", err)
// 		return
// 	}
// 	defer f.Close()

// 	// Set schema
// 	schema := `
// 	{
// 		"Tag": "name=schema",
// 		"Fields": [
// 			{
// 				"Tag": "name=id, type=INT64, repetitiontype=REQUIRED"
// 			},
// 			{
// 				"Tag": "name=data, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
// 			}
// 		]
// 	}
// 	`

// 	// Create parquet writer
// 	pw, err := writer.NewJSONWriter(schema, f, 4)
// 	if err != nil {
// 		log.Println("Error creating Parquet writer:", err)
// 		return
// 	}
// 	defer pw.WriteStop()

// 	// Write each record as a JSON string
// 	for _, row := range df.Records() {
// 		data, _ := jsoniter.Marshal(row) // Marshal row to JSON
// 		if err := pw.Write(string(data)); err != nil {
// 			log.Println("Error writing to Parquet:", err)
// 			return
// 		}
// 	}
// }

// // Main function to initiate fetching, processing and writing data from all shards
// func main() {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	var wg sync.WaitGroup
// 	dataChan := make(chan Row, batchSize)

// 	// The query to fetch data (adjust as needed)
// 	query := "SELECT * FROM big_table_1"

// 	// Fetch data from all shards in parallel
// 	for dbName := range dbConnections {
// 		wg.Add(1)
// 		go fetchDataFromShard(ctx, dbName, query, dataChan, &wg)
// 	}

// 	// Write data to Parquet file in parallel
// 	wg.Add(1)
// 	go writeParquet(ctx, "output/output.parquet", dataChan, &wg)

// 	// Wait for all goroutines to finish
// 	wg.Wait()

// 	log.Println("✅ Data exported successfully to Parquet")
// }
