package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spaghetti-lover/go-db/pkg/kv"
)

func main() {
	fmt.Println("Go-DBMS REPL. Type SQL (INSERT/SELECT) or 'exit' to quit.")

	engine, err := kv.NewWALBPTreeEngine("test.db", "test.wal")
	if err != nil {
		fmt.Println("Init error:", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" || line == "exit" {
			break
		}

		stmt, err := kv.ParseSQL(line)
		if err != nil {
			fmt.Println("Parse error:", err)
			continue
		}

		switch s := stmt.(type) {
		case *kv.InsertStmt:
			key := []byte(s.Vals[0])
			val := []byte(s.Vals[1])
			if err := engine.Set(key, val); err != nil {
				fmt.Println("Insert error:", err)
			} else {
				fmt.Println("OK")
			}
		case *kv.SelectStmt:
			for _, whereVal := range s.Where {
				key := []byte(whereVal)
				val, ok := engine.Get(key)
				if ok {
					fmt.Printf("%s: %s\n", s.Cols[0], string(val))
				} else {
					fmt.Println("Not found")
				}
				break
			}
		default:
			fmt.Println("Unsupported statement")
		}
	}
}
