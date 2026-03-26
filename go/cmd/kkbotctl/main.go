package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: kkbotctl doctor [--database-url ...] [--panel-url ...] [--legacy-sqlite ...]")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "doctor":
		doctor()
	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
		os.Exit(2)
	}
}

func doctor() {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL DSN")
	panelURL := fs.String("panel-url", os.Getenv("PANEL_BASE"), "Panel URL")
	legacySQLite := fs.String("legacy-sqlite", os.Getenv("LEGACY_SQLITE_PATH"), "Legacy SQLite path")
	_ = fs.Parse(os.Args[2:])

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if *databaseURL == "" {
		fmt.Println("[WARN] DATABASE_URL is empty")
	} else if err := pingPostgres(ctx, *databaseURL); err != nil {
		fmt.Printf("[ERROR] PostgreSQL check failed: %v\n", err)
	} else {
		fmt.Println("[OK] PostgreSQL reachable")
	}

	if *panelURL == "" {
		fmt.Println("[INFO] PANEL_BASE is empty")
	} else if err := pingHTTP(*panelURL); err != nil {
		fmt.Printf("[ERROR] Panel check failed: %v\n", err)
	} else {
		fmt.Println("[OK] Panel reachable")
	}

	if *legacySQLite == "" {
		fmt.Println("[INFO] LEGACY_SQLITE_PATH is empty")
	} else if _, err := os.Stat(*legacySQLite); err != nil {
		fmt.Printf("[WARN] Legacy SQLite missing: %v\n", err)
	} else {
		fmt.Println("[OK] Legacy SQLite file found")
	}
}

func pingPostgres(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return conn.Ping(ctx)
}

func pingHTTP(url string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
