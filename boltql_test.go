package boltql

import (
	"fmt"
	"os"
	"testing"
)

const (
	DB_FILE    = "test.db"
	TABLE_NAME = "table"
	INDEX_1    = "index1"
	INDEX_2    = "index2"
)

var (
	db    *DataStore
	table *Table
)

///////////////////////////////////////////////////////////////

//
// Normally a "record" would be a "struct" with named fields
// and the *FieldList() methods would convert the struct to a slice and back.
//
// But this works too :)
//
type TestRecord []interface{}

func (r *TestRecord) ToFieldList() []interface{} {
	return *r
}

func (r *TestRecord) FromFieldList(l []interface{}) {
	*r = l
}

///////////////////////////////////////////////////////////////

func getTable(t *testing.T) *Table {
	if table == nil {
		t.FailNow()
	}

	return table
}

func TestMain(m *testing.M) {
	//
	// open db
	//
	if _db, err := Open(DB_FILE); err != nil {
		panic("cannot open database")
	} else {
		db = _db
	}

	//
	// run tests
	//
	ret := m.Run()

	//
	// close db
	// (not sure of why a defer f() here is not called)
	//
	if db != nil {
		if err := db.Close(); err != nil {
			fmt.Println("close db:", err)
		}
	}

	if err := os.Remove(DB_FILE); err != nil {
		fmt.Println("remove db:", err)
	}

	os.Exit(ret)
}

func Test_01_CreateTable(t *testing.T) {
	var err error

	table, err = db.CreateTable(TABLE_NAME)

	if err == ALREADY_EXISTS {
		t.Error("create table: table already exist")
	} else if err != nil {
		t.Error("create table:", err)
	}
}

func Test_02_CreateIndex(t *testing.T) {
	if err := getTable(t).CreateIndex(INDEX_1, true, 0, 1); err != nil {
		t.Error("create index:", err)
	}

	if err := getTable(t).CreateIndex(INDEX_2, true, 1, 3); err != nil {
		t.Error("create index:", err)
	}
}

func Test_03_GetTable(t *testing.T) {
	tbl, err := db.GetTable(TABLE_NAME)
	if err != nil {
		t.Error("get table:", err)
	}
	if tbl == nil {
		t.Error("table is nil")
	}

	table = tbl
}

func Test_04_Add_Records(t *testing.T) {
	if _, err := getTable(t).Put(&TestRecord{"test__", 42, "some words", AUTOINCREMENT}); err != nil {
		t.Error("put:", err)
	}

	if _, err := getTable(t).Put(&TestRecord{"alpha_", 99, "hello", AUTOINCREMENT}); err != nil {
		t.Error("put:", err)
	}

	if _, err := getTable(t).Put(&TestRecord{"omega_", 12, "both", AUTOINCREMENT}); err != nil {
		t.Error("put:", err)
	}

	if _, err := getTable(t).Put(&TestRecord{"middle", 1, "not sure", AUTOINCREMENT}); err != nil {
		t.Error("put:", err)
	}

	if _, err := getTable(t).Put(&TestRecord{"test__", 99, "2nd test", AUTOINCREMENT}); err != nil {
		t.Error("put:", err)
	}
}

func Test_05_Scan_Index_1(t *testing.T) {
	var rec TestRecord
	var prev string

	if err := getTable(t).Scan(INDEX_1, true, nil, &rec, func(rec DataRecord, err error) bool {
		trec := rec.(*TestRecord)

		if err != nil {
			t.Error("callback", err)
		}

		if len(*trec) != 4 {
			t.Error("len: expected 4, got", len(*trec))
		}

		t.Logf("auto %v %v", (*trec)[1], (*trec)[3])

		if key, ok := (*trec)[0].([]byte); !ok {
			t.Errorf("key not a []byte: %q", *trec)
			return false
		} else if string(key) < prev {
			t.Error("key", string(key), "prev", prev)
			return false
		} else {
			prev = string(key)
		}

		return true
	}); err != nil {
		t.Error("scan index:", err)
	}
}

func Test_06_Scan_Index_2(t *testing.T) {
	var rec TestRecord
	var prev int64

	if err := getTable(t).Scan(INDEX_2, true, nil, &rec, func(rec DataRecord, err error) bool {
		trec := rec.(*TestRecord)

		if err != nil {
			t.Error("callback", err)
		}

		if len(*trec) != 4 {
			t.Error("len: expected 4, got", len(*trec))
		}

		if key, ok := (*trec)[1].(int64); !ok {
			t.Errorf("key not a int64: %q", *trec)
			return false
		} else if key < prev {
			t.Error("key", key, "prev", prev)
			return false
		} else {
			prev = key
		}

		return true
	}); err != nil {
		t.Error("scan index:", err)
	}
}

func Test_07_Get(t *testing.T) {
	var rec TestRecord

	tests := []TestRecord{
		TestRecord{nil, 42, nil, uint64(1)},
		TestRecord{nil, 99, nil, uint64(2)},
		TestRecord{nil, 12, nil, uint64(3)},
		TestRecord{nil, 1, nil, uint64(4)},
		TestRecord{nil, 99, nil, uint64(5)},
	}

	for _, tr := range tests {
		if err := getTable(t).Get(INDEX_2, &tr, &rec); err != nil {
			t.Error("expected", tr, err)
		} else {
			t.Log(rec)
		}
	}
}

func Test_08_Delete(t *testing.T) {
	tests := []TestRecord{
		TestRecord{nil, 42, nil, uint64(1)},
		//TestRecord{nil, 99, nil, uint64(2)},
		TestRecord{nil, 12, nil, uint64(3)},
		//TestRecord{nil, 1, nil, uint64(4)},
		//TestRecord{nil, 99, nil, uint64(5},
	}

	for _, tr := range tests {
		if err := getTable(t).Delete(INDEX_2, &tr); err != nil {
			t.Error("delete", tr, err)
		} else {
			t.Log("deleted", tr)
		}
	}
}

func Test_99_ForEach(t *testing.T) {
	indices := []string{
		"",
		INDEX_1,
		INDEX_2,
	}

	for _, index := range indices {
		t.Log("content of index", index)

		n := 0

		if err := getTable(t).ForEach(index, func(k, v []byte) error {
			t.Logf("  k:[% x], v:[% x]", k, v)
			n += 1
			return nil
		}); err != nil {
			t.Error("ForEach error", err)
		}

		t.Log("  total", n, "records")
	}
}
