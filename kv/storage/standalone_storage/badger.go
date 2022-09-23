package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
)

func InitDb(storePath string) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.Dir = storePath
	opts.ValueDir = storePath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return db, nil
}
