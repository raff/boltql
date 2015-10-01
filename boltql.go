package boltql

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/gobs/typedbuffer"
)

var (
	NO_TABLE         = bolt.ErrBucketNotFound
	NO_INDEX         = bolt.ErrBucketNotFound
	ALREADY_EXISTS   = bolt.ErrBucketExists
	NO_SCHEMA        = errors.New("no schema for table")
	SCHEMA_CORRUPTED = errors.New("schema corrupted")
	NO_KEY           = errors.New("key not found")
	BAD_VALUES       = errors.New("bad values")
)

type DataStore bolt.DB

type DataRecord interface {
	ToFieldList() []interface{}
	FromFieldList([]interface{})
}

// Open the database (create if it doesn't exist)
func Open(dbfile string) (*DataStore, error) {
	db, err := bolt.Open(dbfile, 0666, nil)
	if err != nil {
		return nil, err
	}

	return (*DataStore)(db), nil
}

// Close the database
func (d *DataStore) Close() {
	d.Close()
}

func indices(name string) []byte {
	return []byte(name + "_idx")
}

func schema(name string) []byte {
	return []byte(name + "_schema")
}

//
// A Table is a container for the table name and indices
//
type Table struct {
	name    string
	indices map[string][]uint64

	d *DataStore
}

func (t *Table) String() string {
	return fmt.Sprintf("Table{name: %q, indices: %v}", t.name, t.indices)
}

//
// Create table if doesn't exist
//
func (d *DataStore) CreateTable(name string) (*Table, error) {
	db := (*bolt.DB)(d)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket(schema(name))
		if err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		return &Table{name: name, indices: map[string][]uint64{}, d: d}, nil
	} else {
		return nil, err
	}
}

// Get table info
func (d *DataStore) GetTable(name string) (*Table, error) {
	db := (*bolt.DB)(d)
	table := Table{name: name, indices: map[string][]uint64{}, d: d}

	err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(name)) == nil {
			return NO_TABLE
		}

		b := tx.Bucket(schema(name))
		if b == nil {
			return NO_SCHEMA
		}

		b.ForEach(func(k, v []byte) error {
			name := string(k)
			fields, err := typedbuffer.DecodeUintArray(v)
			if err != nil {
				return SCHEMA_CORRUPTED
			}

			table.indices[name] = fields
			return nil
		})

		return nil
	})

	if err == nil {
		return &table, nil
	} else {
		return nil, err
	}
}

func (t *Table) CreateIndex(index string, fields ...uint64) error {
	db := (*bolt.DB)(t.d)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schema(t.name))
		if b == nil {
			return NO_TABLE
		}

		enc, err := typedbuffer.Encode(fields)
		if err != nil {
			return BAD_VALUES
		}

		if err := b.Put([]byte(index), enc); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(indices(index)); err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		t.indices[index] = fields
	}

	return err
}

func marshalKeyValue(keys []uint64, fields []interface{}) (key, value []byte, err error) {
	vkey := make([]interface{}, len(keys))
	vval := make([]interface{}, len(fields)-len(keys)) // only the entries not stored as keys

	ki := 0
	vi := 0

	for fi, fv := range fields {
		if len(keys) > 0 && fi == int(keys[0]) {
			vkey[ki] = fv
			ki += 1
			keys = keys[1:]
		} else {
			vval[vi] = fv
			vi += 1
		}
	}

	if len(vkey) > 0 {
		if key, err = typedbuffer.Encode(vkey...); err != nil {
			return
		}
	}

	value, err = typedbuffer.Encode(vval...)
	return
}

// Add a record to the table (using sequential record number)
func (t *Table) Put(rec DataRecord) (uint64, error) {
	db := (*bolt.DB)(t.d)

	var key uint64

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.name))
		if b == nil {
			return NO_TABLE
		}

		fields := rec.ToFieldList()

		data, err := typedbuffer.Encode(fields...)
		if err != nil {
			return err
		}

		key, err = b.NextSequence()
		if err != nil {
			return err
		}

		if err = b.Put(typedbuffer.EncodeUint64(key), data); err != nil {
			return err
		}

		for index, keys := range t.indices {
			ib := tx.Bucket(indices(index))
			if ib == nil {
				return NO_TABLE
			}

			key, val, err := marshalKeyValue(keys, fields)
			if err != nil {
				return err
			}

			if key == nil {
				continue
			}

			if err := ib.Put(key, val); err != nil {
				return err
			}
		}

		return nil
	})

	return key, err
}

// Get a record from the table (using sequential record number)
func (t *Table) Get(key uint64, record DataRecord) error {
	db := (*bolt.DB)(t.d)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.name))
		if b == nil {
			return NO_TABLE
		}

		data := b.Get(typedbuffer.EncodeUint64(key))
		if data == nil {
			return NO_KEY
		}

		fields, err := typedbuffer.DecodeAll(data)
		if err != nil {
			return err
		}

		record.FromFieldList(fields)
		return nil
	})

	return err
}

// Delete a record from the table (using sequential record number)
func (t *Table) Delete(key uint64) error {
	db := (*bolt.DB)(t.d)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.name))
		if b == nil {
			return NO_TABLE
		}

		enkey := typedbuffer.EncodeUint64(key)

		c := b.Cursor()
		k, v := c.Seek(enkey)

		// Seek will return the next key if there is no match
		// so make sure we check we got the right record

		if bytes.Equal(enkey, k) {
			if err := c.Delete(); err != nil {
				return err
			}

			fields, err := typedbuffer.DecodeAll(v)
			if err != nil {
				return err
			}

			for index, keys := range t.indices {
				b := tx.Bucket(indices(index))
				if b == nil {
					continue
				}

				vkey := make([]interface{}, len(keys))
				for i, j := range keys {
					vkey[i] = fields[j]
				}

				dkey, err := typedbuffer.Encode(vkey...)
				if err != nil {
					return err
				}

				if err := b.Delete(dkey); err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

// Get all records sorted by sequential id (ascending or descending)
// Call user function with key (position) and record content
func (t *Table) ScanSequential(ascending bool, res DataRecord, callback func(uint64, DataRecord, error) bool) error {
	db := (*bolt.DB)(t.d)

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.name))
		if b == nil {
			return NO_TABLE
		}

		c := b.Cursor()

		// ascending
		first := c.First
		next := c.Next

		// descending
		if !ascending {
			first = c.Last
			next = c.Prev
		}

		for k, v := first(); k != nil; k, v = next() {
			pos, _, _ := typedbuffer.Decode(k)

			fields, err := typedbuffer.DecodeAll(v)
			if err == nil {
				res.FromFieldList(fields)
			}

			if !callback(pos.(uint64), res, err) {
				break
			}
		}

		return nil
	})
}

// Get all records sorted by index (ascending or descending)
// Call user function with record content
func (t *Table) ScanIndex(index string, ascending bool, start, res DataRecord, callback func(DataRecord, error) bool) error {
	db := (*bolt.DB)(t.d)

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(indices(index))
		if b == nil {
			return NO_INDEX
		}

		c := b.Cursor()

		keys := t.indices[index]

		var k, v []byte

		if start != nil {
			key, _, err := marshalKeyValue(keys, start.ToFieldList())
			if err != nil {
				return err
			}

			if key != nil {
				k, v = c.Seek(key)
				if !ascending && !bytes.Equal(key, k) {
					// if descending and keys don't match we want to start from the first key
					// in range (previous)

					k, v = c.Prev()
				}
			}
		}

		if k == nil {
			if ascending {
				k, v = c.First()
			} else {
				k, v = c.Last()
			}
		}

		var next func() (key []byte, value []byte)

		if ascending {
			next = c.Next
		} else {
			next = c.Prev
		}

		for ; k != nil; k, v = next() {
			vkey, err := typedbuffer.DecodeAll(k)
			if err != nil {
				return err
			}

			vval, err := typedbuffer.DecodeAll(v)
			if err != nil {
				return err
			}

			lkey := len(vkey)
			lval := len(vval)

			fields := []interface{}{}

			var ival interface{}

			ik := 0
			lk := len(keys)

			for i := 0; i < lkey+lval; i++ {
				if ik < lk && i == int(keys[ik]) {
					ival = vkey[0]
					vkey = vkey[1:]
					ik += 1
				} else {
					ival = vval[0]
					vval = vval[1:]
				}

				fields = append(fields, ival)
			}

			res.FromFieldList(fields)

			if !callback(res, err) {
				break
			}
		}

		return nil
	})
}

func (t *Table) ForEach(index string, callback func(k, b []byte) error) error {
	db := (*bolt.DB)(t.d)

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(indices(index))
		if b == nil {
			return NO_INDEX
		}

		return b.ForEach(callback)
	})
}
