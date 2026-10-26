// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/limits"
)

type Config struct {
	Address, Database, Table, User, Password string
}

type Client struct {
	conn  driver.Conn
	gate  *limits.Gate
	table string
}

// Identifier restricts configured database/table names. User input is never an
// identifier. Reject quoting and qualification rather than trying to escape it.
func Identifier(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i, r := range s {
		if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

func Open(config Config) (*Client, error) {
	if !Identifier(config.Database) || !Identifier(config.Table) || config.Address == "" || config.User == "" || config.User == "default" {
		return nil, errors.New("invalid restricted ClickHouse configuration")
	}
	gate, err := limits.New(2, 32)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address}, Protocol: clickhouse.Native,
		Auth:         clickhouse.Auth{Database: config.Database, Username: config.User, Password: config.Password},
		MaxOpenConns: 2, MaxIdleConns: 2, DialTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, ConnMaxLifetime: 5 * time.Minute,
		BlockBufferSize: 1,
		Settings: clickhouse.Settings{
			"readonly": 1, "max_threads": 1, "max_execution_time": 15,
			"max_memory_usage": 134217728, "max_result_rows": 10000,
			"max_result_bytes": 4194304, "result_overflow_mode": "throw",
			"max_rows_to_read": 2000000, "read_overflow_mode": "throw",
		},
	})
	if err != nil {
		return nil, errors.New("cannot initialize ClickHouse connection")
	}
	return &Client{conn: conn, gate: gate, table: config.Database + "." + config.Table}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Ready exercises SELECT permission on the actual table and timestamp decoding.
// An empty table is valid; an unauthorized or unreachable table is not ready.
// It consumes the same bounded capacity as application queries will use.
func (c *Client) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	rows, err := retryBusy(ctx, func() (driver.Rows, error) {
		return c.conn.Query(chcontext.ReadOnly(ctx), "SELECT Timestamp FROM "+c.table+" LIMIT 1")
	})
	if err != nil {
		return errors.New("ClickHouse readiness query failed")
	}
	defer rows.Close()
	for rows.Next() {
		var timestamp time.Time
		if err := rows.Scan(&timestamp); err != nil {
			return errors.New("ClickHouse timestamp schema mismatch")
		}
	}
	if rows.Err() != nil {
		return errors.New("ClickHouse readiness result failed")
	}
	return nil
}
