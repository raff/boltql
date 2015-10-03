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

type TestRecord []interface{}

func (r *TestRecord) ToFieldList() []interface{} {
	return *r
}

func (r *TestRecord) FromFieldList(l []interface{}) {
	*r = l
}

///////////////////////////////////////////////////////////////

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
	_, err := db.CreateTable(TABLE_NAME)

	if err == ALREADY_EXISTS {
		t.Error("create table: table already exist")
	} else if err != nil {
		t.Error("create table:", err)
	}
}

func Test_02_GetTable(t *testing.T) {
	tbl, err := db.GetTable(TABLE_NAME)
	if err != nil {
		t.Error("get table:", err)
	}
	if tbl == nil {
		t.Error("table is nil")
	}

	table = tbl
}

func Test_03_CreateIndex(t *testing.T) {
	if err := table.CreateIndex(INDEX_1, 0, 1); err != nil {
		t.Error("create index:", err)
	}

	if err := table.CreateIndex(INDEX_2, 1, 0); err != nil {
		t.Error("create index:", err)
	}
}

func Test_04_Add_Records(t *testing.T) {
	if _, err := table.Put(&TestRecord{"test__", 42, "some words"}); err != nil {
		t.Error("put:", err)
	}

	if _, err := table.Put(&TestRecord{"alpha_", 99, "hello"}); err != nil {
		t.Error("put:", err)
	}

	if _, err := table.Put(&TestRecord{"omega_", 12, "both"}); err != nil {
		t.Error("put:", err)
	}

	if _, err := table.Put(&TestRecord{"middle", 1, "not sure"}); err != nil {
		t.Error("put:", err)
	}

	if _, err := table.Put(&TestRecord{"test__", 99, "2nd test"}); err != nil {
		t.Error("put:", err)
	}
}

func Test_05_Scan_Sequential(t *testing.T) {
	var rec TestRecord
	var prev uint64

	if err := table.ScanSequential(true, &rec, func(row_id uint64, rec DataRecord, err error) bool {
		trec := rec.(*TestRecord)

		if err != nil {
			t.Error("callback", err)
		}

		if row_id <= prev {
			t.Error("row_id", row_id, "prev", prev)
		}

		if len(*trec) != 3 {
			t.Error("len: expected 3, got", len(*trec))
		}

		prev = row_id
		return true
	}); err != nil {
		t.Error("scan sequential:", err)
	}
}

func Test_06_Scan_Index_1(t *testing.T) {
	var rec TestRecord
	var prev string

	if err := table.ScanIndex(INDEX_1, true, nil, &rec, func(rec DataRecord, err error) bool {
		trec := rec.(*TestRecord)

		if err != nil {
			t.Error("callback", err)
		}

		if len(*trec) != 3 {
			t.Error("len: expected 3, got", len(*trec))
		}

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

func Test_07_Scan_Index_2(t *testing.T) {
	var rec TestRecord
	var prev int64

	if err := table.ScanIndex(INDEX_2, true, nil, &rec, func(rec DataRecord, err error) bool {
		trec := rec.(*TestRecord)

		if err != nil {
			t.Error("callback", err)
		}

		if len(*trec) != 3 {
			t.Error("len: expected 3, got", len(*trec))
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
