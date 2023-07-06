package main

import (
	"C"
	"log"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/ydb-platform/fluent-bit-ydb/config"
	"github.com/ydb-platform/fluent-bit-ydb/model"
	"github.com/ydb-platform/fluent-bit-ydb/storage"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Println("[ydb] Exporting plugin.")
	return output.FLBPluginRegister(def, "ydb", "YDB storage")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	cfg, err := config.ReadConfigFromPlugin(plugin)

	if err != nil {
		log.Printf("[ydb] Failed read config: %v\n", err)
		return output.FLB_ERROR
	}

	s, err := storage.New(cfg)

	if err != nil {
		log.Printf("[ydb] Failed create new storage: %v\n", err)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, s)
	log.Println("[ydb] Exported plugin successfully.")
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Println("[ydb] Flush called for unknown instance.")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	s, ok := output.FLBPluginGetContext(ctx).(storage.Storager)
	if !ok {
		return output.FLB_ERROR
	}

	dec := output.NewDecoder(data, int(length))
	count := 0
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
		count++
	}

	err := s.Write(events)

	if err != nil {
		log.Printf("[ydb] Failed insert data: %v\n", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Println("[ydb] Exit called for unknown instance.")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	s, ok := output.FLBPluginGetContext(ctx).(storage.Storager)
	if !ok {
		log.Println("[ydb] Failed get storage.Storager object from context.")
		return output.FLB_ERROR
	}
	err := s.Exit()
	if err != nil {
		log.Println("[ydb] Failed process storage exiting.")
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Println("[ydb] Unregister called.")
	output.FLBPluginUnregister(def)
}

func main() {
}
