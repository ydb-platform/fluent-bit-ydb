package main

import (
	"C" //nolint:gocritic
	"fmt"
	"time"
	"unsafe" //nolint:gocritic

	"github.com/fluent/fluent-bit-go/output"

	"github.com/ydb-platform/fluent-bit-ydb/internal/config"
	"github.com/ydb-platform/fluent-bit-ydb/internal/log"
	"github.com/ydb-platform/fluent-bit-ydb/internal/model"
	"github.com/ydb-platform/fluent-bit-ydb/internal/storage"
)

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "ydb", "YDB storage (version = "+version+")")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	cfg, err := config.ReadConfigFromPlugin(plugin)
	if err != nil {
		log.Error(fmt.Sprintf("failed read config: %v", err))

		return output.FLB_ERROR
	}

	s, err := storage.New(&cfg)
	if err != nil {
		log.Error(fmt.Sprintf("failed create new storage: %v", err))

		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, s)

	log.Info("version=" + version)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	s, ok := output.FLBPluginGetContext(ctx).(interface {
		Write(event []*model.Event) error
	})
	if !ok {
		return output.FLB_ERROR
	}

	dec := output.NewDecoder(data, int(length))

	var events []*model.Event

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			timestamp = time.Now()
		}

		message := make(map[string]interface{})

		for k, v := range record {
			key, ok := k.(string)
			if !ok {
				log.Warn(fmt.Sprintf("unknown type of key '%+v'", k))

				continue
			}
			message[key] = v
		}

		event := &model.Event{
			Timestamp: timestamp,
			Metadata:  C.GoString(tag),
			Message:   message,
		}

		events = append(events, event)
	}

	err := s.Write(events)
	if err != nil {
		log.Error(fmt.Sprintf("write events failed: %v", err))

		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	s, ok := output.FLBPluginGetContext(ctx).(interface {
		Exit() error
	})
	if !ok {
		log.Error("unknown storage object")

		return output.FLB_ERROR
	}
	err := s.Exit()
	if err != nil {
		log.Error(fmt.Errorf("exit failed: %w", err).Error())

		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	output.FLBPluginUnregister(def)
}

func main() {
}
