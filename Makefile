test:
	go test -v ./...

test-coverage:
	rm -rf coverage
	mkdir coverage
	go test ./... -coverpkg=./... -coverprofile coverage/cover.out
	go tool cover -html=coverage/cover.out