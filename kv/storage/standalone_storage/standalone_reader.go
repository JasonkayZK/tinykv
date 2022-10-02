package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorage) NewStandaloneReader() (*StandaloneReader, error) {
	txn := s.Engines.Kv.NewTransaction(false)
	return &StandaloneReader{txn: txn}, nil
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return val, nil
}

func (r *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}
