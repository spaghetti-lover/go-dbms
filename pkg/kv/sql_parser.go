// stmt, err := ParseSQL("INSERT INTO users (id, name) VALUES (1, 'Alice');")
// stmt.(*InsertStmt) => Table: "users", Cols: ["id", "name"], Vals: ["1", "Alice"]

// stmt, err := ParseSQL("SELECT id, name FROM users WHERE id = 1;")
// stmt.(*SelectStmt) => Table: "users", Cols: ["id", "name"], Where: map[string]string{"id": "1"}
package kv

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type SQLStmt interface{}

type InsertStmt struct {
	Table string
	Cols  []string
	Vals  []string
}

type SelectStmt struct {
	Table string
	Cols  []string
	Where map[string]string
}

var (
	insertRe = regexp.MustCompile(`(?i)^INSERT INTO (\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*;?$`)
	selectRe = regexp.MustCompile(`(?i)^SELECT ([^ ]+) FROM (\w+)(?: WHERE (.+))?\s*;?$`)
)

func ParseSQL(sql string) (SQLStmt, error) {
	sql = strings.TrimSpace(sql)
	if m := insertRe.FindStringSubmatch(sql); m != nil {
		cols := splitCSV(m[2])
		vals := splitCSV(m[3])
		if len(cols) != len(vals) {
			return nil, errors.New("column/value count mismatch")
		}
		return &InsertStmt{
			Table: m[1],
			Cols:  cols,
			Vals:  vals,
		}, nil
	}
	if m := selectRe.FindStringSubmatch(sql); m != nil {
		cols := splitCSV(m[1])
		where := map[string]string{}
		if m[3] != "" {
			conds := strings.Split(m[3], "AND")
			for _, cond := range conds {
				parts := strings.SplitN(strings.TrimSpace(cond), "=", 2)
				if len(parts) == 2 {
					where[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
				}
			}
		}
		return &SelectStmt{
			Table: m[2],
			Cols:  cols,
			Where: where,
		}, nil
	}
	return nil, fmt.Errorf("unsupported SQL: %s", sql)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
