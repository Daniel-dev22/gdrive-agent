package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestMemLimitWiredBeforeServers pins the wiring: main() must call
// setupMemLimit (which calls memlimit.Apply) before opening the database or
// starting the HTTP server, so the limit is in force before any real work.
func TestMemLimitWiredBeforeServers(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var mainBody *ast.BlockStmt
	appliesMemlimit := false
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fd.Name.Name {
		case "main":
			mainBody = fd.Body
		case "setupMemLimit":
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == "memlimit" && sel.Sel.Name == "Apply" {
							appliesMemlimit = true
						}
					}
				}
				return true
			})
		}
	}
	if mainBody == nil {
		t.Fatal("main() not found")
	}
	if !appliesMemlimit {
		t.Fatal("setupMemLimit does not call memlimit.Apply")
	}

	var memPos, dbPos, srvPos token.Pos
	ast.Inspect(mainBody, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := c.Fun.(type) {
		case *ast.Ident:
			if fn.Name == "setupMemLimit" && memPos == 0 {
				memPos = c.Pos()
			}
		case *ast.SelectorExpr:
			if id, ok := fn.X.(*ast.Ident); ok && id.Name == "db" && fn.Sel.Name == "Open" && dbPos == 0 {
				dbPos = c.Pos()
			}
			if fn.Sel.Name == "ListenAndServe" && srvPos == 0 {
				srvPos = c.Pos()
			}
		}
		return true
	})
	if memPos == 0 {
		t.Fatal("main() never calls setupMemLimit")
	}
	if dbPos == 0 || srvPos == 0 {
		t.Fatal("anchor calls (db.Open / ListenAndServe) not found; update this test")
	}
	if memPos > dbPos || memPos > srvPos {
		t.Fatal("setupMemLimit must run before db.Open and ListenAndServe")
	}
}
