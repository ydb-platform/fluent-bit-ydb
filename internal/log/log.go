package log

import (
	"fmt"
	"time"
)

var (
	timeLayout = "2006/01/02 15:04:05"
	format     = "[%s] [%s] [\x1b[44mydb\u001B[0m] %s\n"
)

func Debug(msg string) {
	fmt.Printf(format, time.Now().Format(timeLayout), "\033[100mdebug\u001B[0m", msg)
}

func Warn(msg string) {
	fmt.Printf(format, time.Now().Format(timeLayout), "\u001B[30m\u001B[103m warn\u001B[0m", msg)
}

func Info(msg string) {
	fmt.Printf(format, time.Now().Format(timeLayout), "\033[106m info\u001B[0m", msg)
}

func Error(msg string) {
	fmt.Printf(format, time.Now().Format(timeLayout), "\033[101merror\u001B[0m", msg)
}

func Fatal(msg string) {
	fmt.Printf(format, time.Now().Format(timeLayout), "\033[101mfatal\u001B[0m", msg)
}
