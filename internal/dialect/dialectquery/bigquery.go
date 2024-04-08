package dialectquery

import (
	"fmt"
)

type Bigquery struct{}

var _ Querier = (*Bigquery)(nil)

func (p *Bigquery) CreateTable(tableName string) string {
	q := `CREATE TABLE %s (
		version_id integer NOT NULL,
		is_applied boolean NOT NULL,
		tstamp timestamp default CURRENT_TIMESTAMP()
	)
	`
	return fmt.Sprintf(q, tableName)
}

func (p *Bigquery) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied) VALUES ($1, $2)`
	return fmt.Sprintf(q, tableName)
}

func (p *Bigquery) DeleteVersion(tableName string) string {
	q := `DELETE FROM %s WHERE version_id=$1`
	return fmt.Sprintf(q, tableName)
}

func (p *Bigquery) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1`
	return fmt.Sprintf(q, tableName)
}

func (p *Bigquery) ListMigrations(tableName string) string {
	/*	if strings.Count(tableName, ".") == 1 {
		items := strings.Split(tableName, ".")
		tableName = items[len(items)-1]
	}*/
	q := `SELECT version_id, is_applied from %s ORDER BY tstamp DESC`
	return fmt.Sprintf(q, tableName)
}
