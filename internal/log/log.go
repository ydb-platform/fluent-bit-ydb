package log

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetLevel(lvl zerolog.Level) {
	zerolog.SetGlobalLevel(lvl)
}

func Debug(msg string) {
	log.Debug().Msg(msg)
}

func Warn(msg string) {
	log.Warn().Msg(msg)
}

func Info(msg string) {
	log.Info().Msg(msg)
}

func Error(msg string) {
	log.Error().Msg(msg)
}

func Fatal(msg string) {
	log.Fatal().Msg(msg)
}
