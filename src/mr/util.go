package mr

import "fmt"

const debug = false

func Print(a ...interface{}) {
	if debug {
		fmt.Println(a...)
	}
}
