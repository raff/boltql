package boltql

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

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

	// this is just a marker for auto-increment fields
	AUTOINCREMENT = &struct{}{}
)

//
// A DataStore is the main interface to a BoltDB database
//
type DataStore bolt.DB

//
// A DataRecord is the interface for elements that can be stored in a table.
// A DataRecord needs to implement two methods:
//
// ToFieldList() should convert the record fields into a list of values, preserving their order
// (i.e. the same fields should always be in the same position)
//
// FromFieldList() should fill the record with values from the input list. The order of values
// is the same as what was returned from ToFieldList()
//
type DataRecord interface {
	ToFieldList() []interface{}
	FromFieldList([]interface{})
}

//
// Open the database (create if it doesn't exist)
//
func Open(dbfile string) (*DataStore, error) {
	db, err := bolt.Open(dbfile, 0666, nil)
	if err != nil {
		return nil, err
	}

	return (*DataStore)(db), nil
}

//
// Close the database
//
func (d *DataStore) Close() error {
	db := (*bolt.DB)(d)
	return db.Close()
}

func indices(name string) []byte {
	return []byte(name + "_idx")
}

func schema(name string) []byte {
	return []byte(name)
}

//
// A Table is a container for the table name and indices
//
type Table struct {
	name    string
	indices map[string]iplist

	d *DataStore
}

//
// Implement the Stringer interface
//
func (t *Table) String() string {
	return fmt.Sprintf("Table{name: %q, indices: %v}", t.name, t.indices)
}

type indexpos struct {
	field uint
	pos   uint
}

type iplist []indexpos

func makeIndexPos(fields []uint64) iplist {
	ip := make(iplist, len(fields))

	for i, f := range fields {
		ip[i].field = uint(f)
		ip[i].pos = uint(i)
	}

	sort.Sort(ip)
	return ip
}

func (ip iplist) Len() int {
	return len(ip)
}

func (ip iplist) Less(i, j int) bool {
	return ip[i].field < ip[j].field
}

func (ip iplist) Swap(i, j int) {
	ip[i], ip[j] = ip[j], ip[i]
}

//
// Create table if doesn't exist
//
func (d *DataStore) CreateTable(name string) (*Table, error) {
	db := (*bolt.DB)(d)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(schema(name))
		if err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		return &Table{name: name, indices: map[string]iplist{}, d: d}, nil
	} else {
		return nil, err
	}
}

//
// Get existing Table
//
func (d *DataStore) GetTable(name string) (*Table, error) {
	db := (*bolt.DB)(d)
	table := Table{name: name, indices: map[string]iplist{}, d: d}

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(schema(name))
		if b == nil {
			return NO_TABLE
		}

		b.ForEach(func(k, v []byte) error {
			name := string(k)
			fields, err := typedbuffer.DecodeUintArray(v)
			if err != nil {
				return SCHEMA_CORRUPTED
			}

			table.indices[name] = makeIndexPos(fields)
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

//
// Create an index given the name (index) and a list of field positions
// used to create a composite key. The field position should corrispond
// to the entries in DataRecord ToFieldList() and FromFieldList()
//
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
		t.indices[index] = makeIndexPos(fields)
	}

	return err
}

func marshalKeyValue(keys iplist, fields []interface{}) (key, value []byte, err error) {
	if len(keys) == 0 {
		return
	}

	vkey := make([]interface{}, len(keys))
	vval := make([]interface{}, 0)

	kk, lk := 0, len(keys)

	for fi, fv := range fields {
		if kk < lk && uint(fi) == keys[kk].field {
			vkey[keys[kk].pos] = fv
			kk += 1
		} else {
			vval = append(vval, fv)
		}
	}

	if len(vkey) > 0 {
		if key, err = typedbuffer.Encode(vkey...); err != nil {
			return
		}
	}

	if len(vval) > 0 {
		value, err = typedbuffer.Encode(vval...)
	}

	return
}

func unmarshalKeyValue(keys iplist, k, v []byte) ([]interface{}, error) {
	vkey, err := typedbuffer.DecodeAll(k)
	if err != nil {
		return nil, err
	}

	vval, err := typedbuffer.DecodeAll(v)
	if err != nil {
		return nil, err
	}

	lkey := len(vkey)
	lval := len(vval)

	fields := []interface{}{}

	var ival interface{}

	kk, lk := 0, len(keys)

	for i := 0; i < lkey+lval; i++ {
		if kk < lk && uint(i) == keys[kk].field {
			ival = vkey[keys[kk].pos]
			kk += 1
		} else {
			ival = vval[0]
			vval = vval[1:]
		}

		fields = append(fields, ival)
	}

	return fields, nil
}

//
// Add a record to the table (using sequential record number).
// Also add/update records in all indices.
//
func (t *Table) Put(rec DataRecord) (uint64, error) {
	db := (*bolt.DB)(t.d)

	var key uint64

	err := db.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(t.name))
		if b == nil {
			return NO_TABLE
		}

		fields := rec.ToFieldList()

		for i := range fields {
			if fields[i] == AUTOINCREMENT {
				fields[i], err = b.NextSequence()
				if err != nil {
					return err
				}
			}
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

//
// Get a record from the table, given the index and the key
//
func (t *Table) Get(index string, key, res DataRecord) error {
	db := (*bolt.DB)(t.d)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(indices(index))
		if b == nil {
			return NO_INDEX
		}

		c := b.Cursor()

		keys := t.indices[index]

		sk, _, err := marshalKeyValue(keys, key.ToFieldList())
		if err != nil {
			return err
		}

		if sk == nil {
			fmt.Println("marshal returned no key")
			return NO_KEY
		}

		resk, resv := c.Seek(sk)
		if !bytes.Equal(sk, resk) {
			fmt.Printf("expected % x got % x\n", sk, resk)
			return NO_KEY
		}

		fields, err := unmarshalKeyValue(keys, resk, resv)
		if err != nil {
			return err
		}

		res.FromFieldList(fields)
		return nil
	})

	return err
}

//
// Delete a record from the table (using sequential record number)
//
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
				for _, ip := range keys {
					vkey[ip.pos] = fields[ip.field]
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

//
// Get all records sorted by index (ascending or descending)
// Call user function with record content or error
//
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
			fields, err := unmarshalKeyValue(keys, k, v)
			if err != nil {
				return err
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
