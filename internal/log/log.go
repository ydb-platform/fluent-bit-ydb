package log

import (
	"os"

	"github.com/rs/zerolog"
)

var logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.InfoLevel)

func SetLevel(lvl zerolog.Level) {
	logger = logger.Level(lvl)
}

func Debug(msg string) {
	logger.Debug().Msg(msg)
}

func Warn(msg string) {
	logger.Warn().Msg(msg)
}

func Info(msg string) {
	logger.Info().Msg(msg)
}

func Error(msg string) {
	logger.Error().Msg(msg)
}

func Fatal(msg string) {
	logger.Fatal().Msg(msg)
}
