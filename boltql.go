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

func (d *DataStore) SetBulk(b bool) {
	db := (*bolt.DB)(d)
	db.NoSync = b
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
	indices map[string]indexinfo

	d *DataStore
}

//
// Implement the Stringer interface
//
func (t *Table) String() string {
	return fmt.Sprintf("Table{name: %q, indices: %v}", t.name, t.indices)
}

type indexinfo struct {
	nilFirst bool
	iplist   []indexpos
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
		return &Table{name: name, indices: map[string]indexinfo{}, d: d}, nil
	} else {
		return nil, err
	}
}

//
// Get existing Table
//
func (d *DataStore) GetTable(name string) (*Table, error) {
	db := (*bolt.DB)(d)
	table := Table{name: name, indices: map[string]indexinfo{}, d: d}

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(schema(name))
		if b == nil {
			return NO_TABLE
		}

		b.ForEach(func(k, v []byte) error {
			name := string(k)

			nilFirst, rest, err := typedbuffer.Decode(v)
			if err != nil {
				return SCHEMA_CORRUPTED
			}
			fields, err := typedbuffer.DecodeUintArray(rest)
			if err != nil {
				return SCHEMA_CORRUPTED
			}

			table.indices[name] = indexinfo{
				nilFirst: nilFirst.(bool),
				iplist:   makeIndexPos(fields),
			}

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
// used to create a composite key.
//
// nilFirst specifies if nil values should sort first (lowest possible value) or last (highest possible value)
//
// The field position should corrispond to the entries in DataRecord ToFieldList() and FromFieldList()
//
func (t *Table) CreateIndex(index string, nilFirst bool, fields ...uint64) error {
	db := (*bolt.DB)(t.d)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schema(t.name))
		if b == nil {
			return NO_TABLE
		}

		b1, err := typedbuffer.Encode(nilFirst)
		if err != nil {
			return BAD_VALUES
		}
		b2, err := typedbuffer.Encode(fields)
		if err != nil {
			return BAD_VALUES
		}

		enc := append(b1, b2...)
		if err := b.Put([]byte(index), enc); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(indices(index)); err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		t.indices[index] = indexinfo{
			nilFirst: nilFirst,
			iplist:   makeIndexPos(fields),
		}
	}

	return err
}

//
// marshal an array of fields into a key and value pair of encoded values
//
// the key is a composed key of the fields described in info.iplist (field number and order)
// the value is a collection of the remaning fields
//
func (info indexinfo) marshalKeyValue(fields []interface{}) (key, value []byte, err error) {
	if len(info.iplist) == 0 {
		return
	}

	vkey := make([]interface{}, len(info.iplist))
	vval := make([]interface{}, 0)

	kk, lk := 0, len(info.iplist)

	for fi, fv := range fields {
		if kk < lk && uint(fi) == info.iplist[kk].field {
			vkey[info.iplist[kk].pos] = fv
			kk += 1
		} else {
			vval = append(vval, fv)
		}
	}

	if len(vkey) > 0 {
		if key, err = typedbuffer.EncodeNils(info.nilFirst, vkey...); err != nil {
			return
		}
	}

	if len(vval) > 0 {
		value, err = typedbuffer.EncodeNils(info.nilFirst, vval...)
	}

	return
}

//
// unmarshal key, value into a list of decoded fields
//
func (info indexinfo) unmarshalKeyValue(k, v []byte) ([]interface{}, error) {
	vkey, err := typedbuffer.DecodeAll(false, k)
	if err != nil {
		return nil, err
	}

	vval, err := typedbuffer.DecodeAll(false, v)
	if err != nil {
		return nil, err
	}

	lkey := len(vkey)
	lval := len(vval)

	fields := []interface{}{}

	var ival interface{}

	kk, lk := 0, len(info.iplist)

	for i := 0; i < lkey+lval; i++ {
		if kk < lk && uint(i) == info.iplist[kk].field {
			ival = vkey[info.iplist[kk].pos]
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
// Add a record to the table, updating all indices.
// If a record with the same key exists, it's updated.
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

		for index, info := range t.indices {
			ib := tx.Bucket(indices(index))
			if ib == nil {
				return NO_TABLE
			}

			key, val, err := info.marshalKeyValue(fields)
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

		info := t.indices[index]

		sk, _, err := info.marshalKeyValue(key.ToFieldList())
		if err != nil {
			return err
		}

		if sk == nil {
			return NO_KEY
		}

		resk, resv := c.Seek(sk)
		if !bytes.Equal(sk, resk) {
			return NO_KEY
		}

		fields, err := info.unmarshalKeyValue(resk, resv)
		if err != nil {
			return err
		}

		res.FromFieldList(fields)
		return nil
	})

	return err
}

//
// Delete a record from the table, given the index and the key
//
func (t *Table) Delete(index string, key DataRecord) error {
	db := (*bolt.DB)(t.d)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(indices(index))
		if b == nil {
			return NO_INDEX
		}

		info := t.indices[index]

		sk, _, err := info.marshalKeyValue(key.ToFieldList())
		if err != nil {
			return err
		}

		if sk == nil {
			return NO_KEY
		}

		c := b.Cursor()
		k, v := c.Seek(sk)

		// Seek will return the next key if there is no match
		// so make sure we check we got the right record

		if bytes.Equal(sk, k) {
			if err := c.Delete(); err != nil {
				return err
			}

			fields, err := info.unmarshalKeyValue(k, v)
			if err != nil {
				return err
			}

			for i, info := range t.indices {
				if i == index {
					// already done
					continue
				}

				b := tx.Bucket(indices(i))
				if b == nil {
					continue
				}

				// could use marshalKeyValue() instead

				vkey := make([]interface{}, len(info.iplist))
				for _, ip := range info.iplist {
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
// Get all records sorted by index keys (ascending or descending)
// Call user function with record content or error
//
func (t *Table) Scan(index string, ascending bool, start, res DataRecord, callback func(DataRecord, error) bool) error {
	db := (*bolt.DB)(t.d)

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(indices(index))
		if b == nil {
			return NO_INDEX
		}

		c := b.Cursor()

		info := t.indices[index]

		var k, v []byte

		if start != nil {
			key, _, err := info.marshalKeyValue(start.ToFieldList())
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
			fields, err := info.unmarshalKeyValue(k, v)
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

//
// Scan through all records in an index. Calls specified callback with key and value (as []byte, not decoded)
//
func (t *Table) ForEach(index string, callback func(k, v []byte) error) error {
	db := (*bolt.DB)(t.d)

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.name))
		if len(index) > 0 {
			b = tx.Bucket(indices(index))
		}

		if b == nil {
			return NO_INDEX
		}

		return b.ForEach(callback)
	})
}
