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
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

type Config struct {
	Address, Database, Table, User, Password string
	Limits                                   Limits
	ServiceNameAttributes                    string
}

type Client struct {
	conn                             driver.Conn
	gate                             *limits.Gate
	table                            string
	limits                           Limits
	service                          sqlgen.ServiceMapping
	pageRows, pageBytes, planBuckets int
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
	service, err := sqlgen.ParseServiceMapping(config.ServiceNameAttributes)
	if err != nil {
		return nil, err
	}
	costs, err := config.Limits.defaults()
	if err != nil {
		return nil, err
	}
	gate, err := limits.New(2, 32)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address}, Protocol: clickhouse.Native,
		Auth:         clickhouse.Auth{Database: config.Database, Username: config.User, Password: config.Password},
		MaxOpenConns: 2, MaxIdleConns: 2, DialTimeout: 5 * time.Second,
		ReadTimeout: costs.QueryTimeout, ConnMaxLifetime: 5 * time.Minute,
		BlockBufferSize: 1,
	})
	if err != nil {
		return nil, errors.New("cannot initialize ClickHouse connection")
	}
	client := &Client{conn: conn, gate: gate, table: config.Database + "." + config.Table, limits: costs, service: service, pageRows: pageRows, pageBytes: pageBytes, planBuckets: planBuckets}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, bytes, err := client.readerProfile(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}
	client.pageRows, client.pageBytes = pageBudgets(rows, bytes)
	client.planBuckets = min(planBuckets, client.pageRows)
	return client, nil
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
	if _, _, err := c.readerProfile(ctx); err != nil {
		return err
	}
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

// readerProfile checks the safety contract without sending settings. Server
// limits are authoritative. Break/any overflow modes can return partial data
// indistinguishable from a complete result and therefore are refused.
func (c *Client) readerProfile(ctx context.Context) (uint64, uint64, error) {
	var readonly uint8
	var rows, bytes uint64
	var resultMode, readMode, groupMode, sortMode, timeoutMode string
	result, err := retryBusy(ctx, func() (driver.Rows, error) {
		return c.conn.Query(chcontext.ReadOnly(ctx), "SELECT getSetting('readonly'), toUInt64(getSetting('max_result_rows')), toUInt64(getSetting('max_result_bytes')), toString(getSetting('result_overflow_mode')), toString(getSetting('read_overflow_mode')), toString(getSetting('group_by_overflow_mode')), toString(getSetting('sort_overflow_mode')), toString(getSetting('timeout_overflow_mode'))")
	})
	if err != nil {
		return 0, 0, errors.New("cannot verify ClickHouse reader profile")
	}
	defer result.Close()
	if !result.Next() {
		return 0, 0, errors.New("empty ClickHouse reader profile")
	}
	if err := result.Scan(&readonly, &rows, &bytes, &resultMode, &readMode, &groupMode, &sortMode, &timeoutMode); err != nil {
		return 0, 0, errors.New("invalid ClickHouse reader profile")
	}
	if result.Next() || result.Err() != nil {
		return 0, 0, errors.New("incomplete ClickHouse reader profile")
	}
	if readonly != 1 || resultMode != "throw" || readMode != "throw" || groupMode != "throw" || sortMode != "throw" || timeoutMode != "throw" {
		return 0, 0, errors.New("ClickHouse reader requires readonly=1 and throwing overflow modes")
	}
	if rows == 1 || bytes > 0 && bytes < 256 {
		return 0, 0, errors.New("ClickHouse result limits are too small")
	}
	return rows, bytes, nil
}

func pageBudgets(rows, bytes uint64) (int, int) {
	r, b := pageRows, pageBytes
	if rows > 0 && rows < uint64(2*r) {
		r = int(rows / 2)
	}
	if bytes > 0 && bytes < uint64(2*b) {
		b = int(bytes / 2)
	}
	return max(1, r), max(1, b)
}
