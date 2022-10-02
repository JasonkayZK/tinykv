package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Done Your Code Here (1).

	log.Infof("RawGet cf=%v, key=%v", req.GetCf(), req.GetKey())

	// Step 1: Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Errorf("Error getting storage reader: %v", err)
		return nil, err
	}
	defer reader.Close()

	// Step 2: Get key
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Errorf("RawGet err: %v", err)
		return nil, err
	}
	if len(val) <= 0 {
		log.Warnf("RawGet nil val: %v", val)
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	// Step 3: Return
	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Done Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	log.Infof("RawPut cf=%v, key=%v, value=%v", req.GetCf(), req.GetKey(), req.GetValue())

	modifyItems := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	}
	err := server.storage.Write(req.Context, modifyItems)
	if err != nil {
		log.Errorf("RawPut err, %v", err)
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Done Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	log.Infof("RawDelete cf=%v, key=%v", req.GetCf(), req.GetKey())

	modify := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		log.Errorf("RawDelete failed, err: %v", err)
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Done Your Code Here (1).
	// Hint: Consider using reader.IterCF

	log.Infof("RawScan cf=%v, start=%v, limit=%v", req.GetCf(), req.GetStartKey(), req.GetLimit())

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Errorf("Error getting storage reader: %v", err)
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	resp := &kvrpcpb.RawScanResponse{}
	iter.Seek(req.StartKey)
	for i := uint32(0); iter.Valid() && i < req.Limit; i += 1 {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			log.Warnf("RawScan items error occurs && key=%v, err=%+v", key, err)
			continue
		}
		resp.Kvs = append(resp.Kvs,
			&kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			},
		)
		iter.Next()
	}

	return resp, nil
}
