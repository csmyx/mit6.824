package raft

import (
	"fmt"
	"log"
	"runtime"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug {
		var x = []interface{}{file_line()}
		x = append(x, a...)
		log.Println(x...)
	}
	return
}

func file_line() string {
	_, file, line, ok := runtime.Caller(2)
	if ok {
		// fmt.Println("func name: " + runtime.FuncForPC(funcName).Name())
		return fmt.Sprintf("[%s:%d]: ", file, line)
	}
	return ""
}
