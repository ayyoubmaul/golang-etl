package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"golang-etl/schema"
	"log"
	"path/filepath"
	"reflect"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type GenericRow map[string]interface{}

var dbs map[string]*sql.DB

var mutex sync.Mutex
var mu sync.Mutex

var rowPool = sync.Pool{
	New: func() interface{} {
		return make(GenericRow)
	},
}

type TableJob struct {
	TableName  string
	Output     string
	PrimaryKey string
	Db         string
}

func fetchDataByKeyRange(ctx context.Context, shard string, job TableJob, startKey, endKey int64, dataChan chan<- GenericRow, fetchWg *sync.WaitGroup, semaphore chan struct{}) {
	defer fetchWg.Done()

	semaphore <- struct{}{}
	defer func() {
		<-semaphore // Release slot when done
	}()

	if startKey > endKey {
		log.Printf("[%s] Invalid key range: startKey %d is greater than endKey %d", shard, startKey, endKey)
		return
	}

	db := dbs[shard]
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v\n", shard, err)
		return
	}
	defer conn.Close()

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s >= ? AND %s < ?", job.TableName, job.PrimaryKey, job.PrimaryKey)
	rows, err := conn.QueryContext(ctx, query, startKey, endKey)
	if err != nil {
		log.Printf("[%s] Query error: %v\n", shard, err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Printf("[%s] Columns error: %v\n", shard, err)
		return
	}

	for rows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("[%s] Scan error: %v\n", shard, err)
				continue
			}

			row := rowPool.Get().(GenericRow)
			for k := range row {
				delete(row, k)
			}

			for i, col := range columns {
				val := values[i]
				if b, ok := val.([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = val
				}
			}

			dataChan <- row
		}
	}

	log.Printf("[%s] Done fetching range %d - %d", shard, startKey, endKey)
}

func parquetWriter(ctx context.Context, outputPrefix string, compression parquet.CompressionCodec, dataChan <-chan GenericRow, writeWg *sync.WaitGroup, semaphore chan struct{}) {
	defer writeWg.Done()

	var fileCounter int

	batchSize := 1000000
	batch := make([]interface{}, 0, batchSize)

	writeBatch := func(batch []interface{}, fileCounter int) {
		// Acquire semaphore slot
		semaphore <- struct{}{}
		defer func() { <-semaphore }()

		outputFile := filepath.Join("output", fmt.Sprintf("%s_batch_%d.parquet", outputPrefix, fileCounter))

		pw, f, err := createParquetWriter(outputFile, compression)
		if err != nil {
			log.Fatalf("Failed to create parquet writer: %v", err)
		}
		defer func() {
			if cerr := f.Close(); cerr != nil {
				log.Printf("File close error: %v", cerr)
			}
		}()

		log.Printf("Writing batch, size=%d to file: %s", len(batch), outputFile)

		for i, r := range batch {
			if r == nil {
				log.Fatalf("batch[%d] is nil", i)
			}
			data, _ := json.Marshal(r)
			if err = pw.Write(string(data)); err != nil {
				log.Fatalf("Write error: %v", err)
			}
		}

		if err := pw.WriteStop(); err != nil {
			log.Fatalf("failed to finalize parquet file: %v", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case row, ok := <-dataChan:
			if !ok {
				// Channel closed
				if len(batch) > 0 {
					fileCounter++
					writeBatch(batch, fileCounter)
				}
				return
			}

			batch = append(batch, row)

			if len(batch) >= batchSize {
				fileCounter++
				writeBatch(batch, fileCounter)
				batch = batch[:0]
			}
		}
	}
}

func getPrimaryKeyRange(job TableJob, dbName string) (int64, int64) {
	db, exists := dbs[dbName]
	if !exists {
		log.Fatalf("Database %s does not exist", dbName)
	}

	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", job.PrimaryKey, job.PrimaryKey, job.TableName)

	log.Println(query)

	var minKey, maxKey int64
	err := db.QueryRow(query).Scan(&minKey, &maxKey)
	if err != nil {
		log.Fatalf("Error getting key range for %s: %v", job.TableName, err)
	}
	return minKey, maxKey
}

func getColumnsFromRow(row GenericRow) ([]string, []reflect.Type) {
	columns := make([]string, 0, len(row))
	columnTypes := make([]reflect.Type, 0, len(row))

	for col, value := range row {
		columns = append(columns, col)
		columnTypes = append(columnTypes, reflect.TypeOf(value))
	}

	return columns, columnTypes
}

func createParquetWriter(output string, compression parquet.CompressionCodec) (*writer.JSONWriter, source.ParquetFile, error) {
	s := schema.MustLoadSchema("schema/db.yaml")
	dbSchema := schema.FormatSchema(s)

	log.Println(dbSchema)

	// db_schema := `
	// {
	// "Tag": "name=schema",
	// "Fields": [
	// 	{
	// 	"Tag": "name=id, type=INT64, repetitiontype=REQUIRED"
	// 	},
	// 	{
	// 	"Tag": "name=data, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
	// 	}
	// ]
	// }
	// `

	f, err := local.NewLocalFileWriter(output)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create output file: %v", err)
	}

	pw, err := writer.NewJSONWriter(dbSchema, f, 4)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create parquet writer: %v", err)
	}

	pw.CompressionType = compression

	return pw, f, nil
}

func main() {
	shards := map[string]string{
		"db_1": "root:root@tcp(127.0.0.1:3307)/jerry",
		"db_2": "root:root@tcp(127.0.0.1:3308)/pikachu",
	}

	dbs = make(map[string]*sql.DB)
	for name, dsn := range shards {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(fmt.Sprintf("DB connect error to %s: %v", name, err))
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		dbs[name] = db
	}

	tables := []TableJob{
		{TableName: "big_table_1", Output: "db_1_big_table_1.parquet", PrimaryKey: "id", Db: "db_1"},
		{TableName: "big_table_1", Output: "db_2_big_table_1.parquet", PrimaryKey: "id", Db: "db_2"},
	}

	for _, table := range tables {
		ctx, cancel := context.WithCancel(context.Background())
		dataChan := make(chan GenericRow, 1000)

		var fetchWg sync.WaitGroup
		var writeWg sync.WaitGroup

		fetchSemaphore := make(chan struct{}, 10)
		writeSemaphore := make(chan struct{}, 10)

		keyRange := int64(1000000) // Define key range to split work

		minKey, maxKey := getPrimaryKeyRange(table, table.Db)

		startKey := minKey
		for startKey < maxKey {
			endKey := startKey + keyRange
			if endKey > maxKey {
				endKey = maxKey + 1
			}

			log.Printf("[%s] Fetching from %d to %d", table.Db, startKey, endKey)

			fetchWg.Add(1)
			go fetchDataByKeyRange(ctx, table.Db, table, startKey, endKey, dataChan, &fetchWg, fetchSemaphore)

			startKey = endKey
		}

		writeWg.Add(1)
		go parquetWriter(ctx, table.Output, parquet.CompressionCodec_SNAPPY, dataChan, &writeWg, writeSemaphore)

		fetchWg.Wait()
		close(dataChan)
		writeWg.Wait()

		cancel()
	}

	log.Println("All tables exported successfully")
}
