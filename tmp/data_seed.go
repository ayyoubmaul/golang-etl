// package main

// import (
// 	"database/sql"
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"strings"
// 	"time"

// 	_ "github.com/go-sql-driver/mysql"
// )

// func randomString(n int) string {
// 	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
// 	b := make([]rune, n)
// 	for i := range b {
// 		b[i] = letters[rand.Intn(len(letters))]
// 	}
// 	return string(b)
// }

// func main() {
// 	shards := map[string]string{
// 		"shard_1": "root:root@tcp(127.0.0.1:3307)/jerry",
// 		"shard_2": "root:root@tcp(127.0.0.1:3308)/pikachu",
// 	}

// 	for name, dsn := range shards {
// 		db, err := sql.Open("mysql", dsn)
// 		if err != nil {
// 			log.Fatalf("[%s] Connection error: %v", name, err)
// 		}
// 		defer db.Close()

// 		_, err = db.Exec(`
//             CREATE TABLE IF NOT EXISTS big_table_1 (
//                 id BIGINT AUTO_INCREMENT PRIMARY KEY,
//                 data VARCHAR(255)
//             )
//         `)
// 		if err != nil {
// 			log.Fatalf("[%s] Create table error: %v", name, err)
// 		}

// 		log.Printf("[%s] Inserting rows...", name)
// 		total := 1_000_000
// 		batchSize := 1000
// 		rand.Seed(time.Now().UnixNano())

// 		for i := 0; i < total; i += batchSize {
// 			var builder strings.Builder
// 			builder.WriteString("INSERT INTO big_table_1 (data) VALUES ")
// 			for j := 0; j < batchSize; j++ {
// 				builder.WriteString(fmt.Sprintf("('%s')", randomString(20)))
// 				if j != batchSize-1 {
// 					builder.WriteString(",")
// 				}
// 			}

// 			_, err := db.Exec(builder.String())
// 			if err != nil {
// 				log.Fatalf("[%s] Insert error: %v", name, err)
// 			}

// 			if (i/batchSize)%10 == 0 {
// 				log.Printf("[%s] Inserted %d rows", name, i+batchSize)
// 			}
// 		}

// 		log.Printf("[%s] Done inserting %d rows", name, total)
// 	}
// }
