.PHONY: help install run-dev run-build clean

help:
	@echo "Pepeunit DataPipe - Commands:"
	@echo ""
	@echo "install:          Install all dependencies"
	@echo "run-dev:          Run dev mod"
	@echo "run-build:        Run build binary"
	@echo "clean:            Clean cache package and data_pipe binary"

install:
	@echo "Install all dependencies"
	go mod download

run-dev:
	@echo "Run dev mod"
	go run main.go

run-build:
	@echo "Run build binary"
	CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o data_pipe .

clean:
	@echo "Clean node_modules and dist..."
	go clean -modcache && rm -rf data_pipe 
