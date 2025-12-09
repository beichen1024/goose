package dialects

import (
	"fmt"
	"strings"

	"github.com/pressly/goose/v3/database/dialect"
)

// NewBigQuery returns a new [dialect.Querier] for BigQuery dialect.
func NewBigQuery() dialect.QuerierExtender {
	return &bigquery{}
}

type bigquery struct{}

var _ dialect.QuerierExtender = (*bigquery)(nil)

func (p *bigquery) CreateTable(tableName string) string {
	q := `CREATE TABLE %s (
		version_id integer NOT NULL,
		is_applied boolean NOT NULL,
		tstamp timestamp default CURRENT_TIMESTAMP()
	)
	`
	return fmt.Sprintf(q, tableName)
}

func (p *bigquery) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied) VALUES (?, ?)`
	return fmt.Sprintf(q, tableName)
}

func (p *bigquery) DeleteVersion(tableName string) string {
	q := `DELETE FROM %s WHERE version_id=?`
	return fmt.Sprintf(q, tableName)
}

func (p *bigquery) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1`
	return fmt.Sprintf(q, tableName)
}

func (p *bigquery) ListMigrations(tableName string) string {
	/*	if strings.Count(tableName, ".") == 1 {
		items := strings.Split(tableName, ".")
		tableName = items[len(items)-1]
	}*/
	q := `SELECT version_id, is_applied from %s ORDER BY tstamp DESC`
	return fmt.Sprintf(q, tableName)
}

func (p *bigquery) GetLatestVersion(tableName string) string {
	q := `SELECT max(version_id) FROM %s`
	return fmt.Sprintf(q, tableName)
}

// TODO: need to update to bigquery grammer
func (p *bigquery) TableExists(tableName string) string {
	datasetName, tableName := parseBigQueryTableIdentifier(tableName)
	if datasetName != "" {
		q := `SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s' )`
		return fmt.Sprintf(q, datasetName, tableName)
	}
	q := `SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE (current_schema() IS NULL OR schemaname = current_schema()) AND tablename = '%s' )`
	return fmt.Sprintf(q, tableName)
}

func parseBigQueryTableIdentifier(name string) (schema, table string) {
	schema, table, found := strings.Cut(name, ".")
	if !found {
		return "", name
	}
	return schema, table
}
