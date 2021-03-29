package main

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/zyguan/xs/rule"
)

const baseDDL = `create table $(TABLE) (
	c01 int,
	c02 bigint,
	c03 tinyint,
	c04 decimal(10,3),
	c05 bit(8),
	c06 year,
	c07 date,
	c08 time,
	c09 datetime,
	c10 timestamp,
	c11 char(20),
	c12 binary(20),
	c13 varchar(20),
	c14 varbinary(20),
	c15 text,
	c16 blob,
	c17 enum('a', 'b', 'c', 'd', 'e'),
	c18 set('a', 'b', 'c', 'd', 'e'),
	$(KEYS)
) $(OPTIONS)`

var baseData = []string{
	"insert into $(TABLE) values (1, 1, 1, 1.234, 0b1100001, 2000, '2000-01-01', '00:00:00', '2000-01-01 00:00:00', '2000-01-01 00:00:00', 'aBcDe', '01234', 'EdCbA', '43210', '0000001', '000000a', 'a', 'a');",
	"insert into $(TABLE) values (2, 2, 2, 2.345, 0b1100010, 2001, '2001-01-01', '01:01:01', '2001-01-01 01:01:01', '2001-01-01 01:01:01', 'bCdEa', '12340', 'DcBaE', '32104', '000001A', '00000ab', 'b', 'a,b');",
	"insert into $(TABLE) values (3, 3, 3, 3.456, 0b1100011, 2002, '2002-01-01', '02:02:02', '2002-01-01 02:02:02', '2002-01-01 02:02:02', 'cDeAb', '23401', 'CbAeD', '21043', '00001a2', '0000abc', 'c', 'a,b,c');",
	"insert into $(TABLE) values (4, 4, 4, 4.567, 0b1100100, 2003, '2003-01-01', '03:03:03', '2003-01-01 03:03:03', '2003-01-01 03:03:03', 'dEaBc', '34012', 'BaEdC', '10432', '0001A2b', '000abcd', 'd', 'a,b,c,d');",
	"insert into $(TABLE) values (5, 5, 5, 5.678, 0b1100101, 2004, '2004-01-01', '04:04:04', '2004-01-01 04:04:04', '2004-01-01 04:04:04', 'eAbCd', '40123', 'AeDcB', '04321', '001a2B3', '00abcde', 'e', 'a,b,c,d,e');",
}

func genCreate(name string, keys string, options string) []string {
	ddl := strings.Replace(baseDDL, "$(TABLE)", name, 1)
	ddl = strings.Replace(ddl, "$(KEYS)", keys, 1)
	ddl = strings.Replace(ddl, "$(OPTIONS)", options, 1)
	return []string{"drop table if exists " + name, ddl}
}

func genInsert(name string) []string {
	ss := make([]string, len(baseData))
	for i, s := range baseData {
		ss[i] = strings.Replace(s, "$(TABLE)", name, 1)
	}
	return ss
}

func genKeys() []string {
	c11 := OneOf("c11", "c11(4)")
	c12 := OneOf("c12", "c12(4)")
	c13 := OneOf("c13", "c13(4)")
	c14 := OneOf("c14", "c14(4)")
	cols := []interface{}{"c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10", c11, c12, c13, c14, "c15(6)", "c16(6)", "c17", "c18"}

	singlePkCol := OneOf(cols...)
	var tuples []interface{}
	for i := 0; i < len(cols)-1; i++ {
		tuples = append(tuples, Seq(cols[i], ", ", OneOf(cols[i+1:]...)))
	}
	multiPkCols := OneOf(tuples...)
	pk := Seq("primary key (", OneOf(singlePkCol, multiPkCols), ") /*T![clustered_index] ", OneOf("clustered", "nonclustered"), " */")

	var (
		ks1 []interface{}
		ks2 []interface{}
	)
	for i, c := range []interface{}{"c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10", "c11", "c12", "c13", "c14", "c15(6)", "c16(6)", "c17", "c18"} {
		if i%2 == 0 {
			ks1 = append(ks1, ", unique key (", c, ")")
			ks2 = append(ks2, ", key (", c, ")")
		} else {
			ks1 = append(ks1, ", key (", c, ")")
			ks2 = append(ks2, ", unique key (", c, ")")
		}
	}
	root := Seq(pk, OneOf(Seq(ks1...), Seq(ks2...)))

	keys := []string{}
	Walk(root, func(xs ...interface{}) {
		keys = append(keys, fmt.Sprint(xs...))
	})
	return keys
}

func genTests() <-chan TestTable {
	ch := make(chan TestTable)
	go func() {
		defer close(ch)
		cnt, name := 0, ""
		for _, k := range genKeys() {
			cnt += 1
			name = "t" + strconv.Itoa(cnt)
			ch <- TestTable{Name: name, Create: genCreate(name, k, "charset=utf8mb4 collate=utf8mb4_bin"), Insert: genInsert(name)}
			cnt += 1
			name = "t" + strconv.Itoa(cnt)
			ch <- TestTable{Name: name, Create: genCreate(name, k, "charset=utf8mb4 collate=utf8mb4_general_ci"), Insert: genInsert(name)}
		}
	}()
	return ch
}
