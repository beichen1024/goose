//go:build !no_bigquery
// +build !no_bigquery

package main

import (
	_ "gorm.io/driver/bigquery"
)
