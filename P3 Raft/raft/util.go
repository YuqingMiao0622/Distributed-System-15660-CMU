package raft

import "log"
import "os"

// Debugging
const Debug = 0

var (
	logFile, _ = os.OpenFile("log.txt", os.O_RDWR|os.O_TRUNC, 0)
	logger     = log.New(logFile, "", log.Lshortfile|log.Lmicroseconds)
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	//	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	if Debug > 0 {
		logger.Printf(format, a...)
		//		log.Printf(format, a...)
	}
	return
}

func NPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
