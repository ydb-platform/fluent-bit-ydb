build:
	go build -buildmode=c-shared -o ${BIN}

clean:
	go clean
	rm -f ${BIN}