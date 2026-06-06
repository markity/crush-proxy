package comm

import "log"

type DebugeLogger struct {
	Logger *log.Logger
	Do     bool
}

func NewDebugeLogger(do bool) *DebugeLogger {
	return &DebugeLogger{
		Logger: &log.Logger{},
		Do:     do,
	}
}

func (l *DebugeLogger) Printf(format string, v ...any) {
	if l.Do {
		log.Printf(format, v...)
	}
}

func (l *DebugeLogger) Println(v ...any) {
	if l.Do {
		log.Println(v...)
	}
}
