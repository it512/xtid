package main

import (
	"fmt"

	"github.com/it512/xtid"
)

var id = xtid.IDGen(17)

func main() {
	fmt.Println(id())
}
