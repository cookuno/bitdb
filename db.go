package bitdb

import "database/sql"

type BitDB struct  {
	db *sql.DB
}

func NewDB(db *sql.DB) *BitDB {
	return &BitDB{db:db}
}

