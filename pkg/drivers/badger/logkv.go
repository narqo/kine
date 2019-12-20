package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/server"
)

const maxRevision = math.MaxInt64

const (
	keyRevPrefix byte = 0b10000000

	keyEvSep byte = '\xff'
)

type LogKV struct {
	db  *badger.DB
	seq *badger.Sequence
}

var _ logstructured.Log = (*LogKV)(nil)

func (log *LogKV) Start(ctx context.Context) error {
	panic("implement me")
}

func (log *LogKV) Append(ctx context.Context, event *server.Event) (newRevision int64, err error) {
	rev, err := log.seq.Next()
	if err != nil {
		return 0, err
	}

	// TODO(narqo): is it even possible to handle this case on the app level; maybe panic is better here?
	if rev >= maxRevision {
		return 0, errors.Errorf("could not append event %v: max revision reached %d", event, rev)
	}

	var entries []*badger.Entry

	var keyRev [9]byte
	keyRev[0] = keyRevPrefix
	binary.BigEndian.PutUint64(keyRev[1:], rev)
	val, err := json.Marshal(event)
	if err != nil {
		return 0, errors.Wrapf(err, "could not encode event %v", event)
	}
	entries = append(entries, badger.NewEntry(keyRev[:], val))

	keyEvRev := make([]byte, 0, len(event.KV.Key)+1+8) // 1 is for separater (byte), 8 is for revision (uint64)
	keyEvRev = append(keyEvRev, event.KV.Key...)
	keyEvRev = append(keyEvRev, keyEvSep)
	keyEvRev = append(keyEvRev, keyRev[:]...)
	entries = append(entries, badger.NewEntry(keyEvRev, nil))

	err = log.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			if err := txn.SetEntry(entry); err != nil {
				return err
			}
		}
		return nil
	})
	return int64(rev), err
}

func (log *LogKV) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*server.Event, error) {
	panic("implement me")
}

func (log *LogKV) After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error) {
	panic("implement me")
}

func (log *LogKV) Watch(ctx context.Context, prefix string) <-chan []*server.Event {
	panic("implement me")
}

func (log *LogKV) CurrentRevision(ctx context.Context) (int64, error) {
	var rev uint64
	err := log.db.View(func(txn *badger.Txn) (err error) {
		rev, err = getCurrentRevision(txn)
		return err
	})
	return int64(rev), err
}

func getCurrentRevision(txn *badger.Txn) (rev uint64, err error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // keys-only iteration
	opts.Reverse = true

	it := txn.NewIterator(opts)
	defer it.Close()

	it.Seek([]byte{keyRevPrefix})

	if it.Valid() {
		revKey := it.Item().Key()
		rev = binary.BigEndian.Uint64(revKey[1:])
	}
	return rev, nil
}

func (log *LogKV) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev   uint64
		count int64
	)
	err := log.db.View(func(txn *badger.Txn) (err error) {
		rev, err = getCurrentRevision(txn)
		if err != nil {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		allKeysGroup := strings.HasSuffix(prefix, "/")
		keyPrefix := []byte(prefix)
		for it.Seek(keyPrefix); it.Valid(); it.Next() {
			keyEv := it.Item().Key()
			key := keyEv[:len(keyEv)-1-8]
			if bytes.Equal(key, keyPrefix) {
				count++
			} else if allKeysGroup && bytes.HasPrefix(key, keyPrefix) {
				count++
			}
		}
		return nil
	})
	return int64(rev), count, err
}
