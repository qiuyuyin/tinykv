package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter    engine_util.DBIterator
	nextKey []byte
	txn     *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		txn:     txn,
		nextKey: startKey,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.nextKey == nil {
		return nil, nil, nil
	}

	var key = scan.nextKey

	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))

	if !scan.iter.Valid() {
		scan.nextKey = nil
		return nil, nil, nil
	}

	var item = scan.iter.Item()
	currUserKey := DecodeUserKey(item.Key())
	// find next key
	for scan.iter.Valid() {
		nextUserKey := DecodeUserKey(scan.iter.Item().Key())
		if !bytes.Equal(nextUserKey, currUserKey) {
			scan.nextKey = nextUserKey
			break
		}
		scan.iter.Next()
	}

	if !scan.iter.Valid() {
		scan.nextKey = nil
	}

	value, err := scan.txn.GetValue(currUserKey)
	if err != nil {
		return nil, nil, err
	}
	return currUserKey, value, nil
}
