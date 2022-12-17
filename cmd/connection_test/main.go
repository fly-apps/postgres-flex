package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/jackc/pgx/v4"
)

func main() {
	uri := flag.String("uri", "", "PG cluster connection string")
	totalWrites := flag.Int("total-writes", 0, "Total writes")

	flag.Parse()

	ctx := context.Background()
	conn, err := openConnection(ctx, *uri)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	if *totalWrites == 0 {
		*totalWrites = 1000
	}

	if _, err := admin.CreateDatabase(ctx, conn, "benchmark", "postgres"); err != nil {
		panic(err)
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS bench ( id serial primary key, val varchar(100));")
	_, err = conn.Exec(ctx, sql)
	if err != nil {
		panic(conn)
	}

	seed := generateSeed()

	for i := 0; i < *totalWrites; i++ {
		val := fmt.Sprintf("%s-%d", seed, i)
		sql := fmt.Sprintf("INSERT INTO bench (val) VALUES ('%s');", val)
		_, err = conn.Exec(ctx, sql)
		if err != nil {
			fmt.Printf("(%d of %d) - Failed\n", i, *totalWrites-1)
			time.Sleep(2)
		}

		fmt.Printf("(%d of %d) - Success\n", i, *totalWrites-1)
	}

}

func generateSeed() string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, 5)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func openConnection(parentCtx context.Context, uri string) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	fmt.Println(uri)
	conf, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, err
	}

	conf.ConnectTimeout = 5 * time.Second
	conn, err := pgx.ConnectConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
