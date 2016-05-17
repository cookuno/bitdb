package bitdb

import "database/sql"

type BitDB struct  {
	db *sql.DB
	driver string
}

func New(db *sql.DB, driver string) *BitDB {
	return &BitDB{db:db, driver:driver}
}

func (self *BitDB) BeginTx() (*Tx, error) {
	sqlTx, txBeginErr := self.db.Begin()
	if txBeginErr != nil {
		return nil, txBeginErr
	}
	self.db.Driver()
	tx := Tx{}
	tx.tx = sqlTx
	tx.driver = self.driver
	return &tx, nil
}

