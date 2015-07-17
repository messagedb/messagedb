.PHONY: all tags clean test build install generate

OK_COLOR=\033[32;01m
NO_COLOR=\033[0m

all: build

build:
	@echo "$(OK_COLOR)==> Building binary...$(NO_COLOR)"
	@go build ./...

test:
	@echo "$(OK_COLOR)==> Running tests...$(NO_COLOR)"
	@go test ./...

install: build
	@echo "$(OK_COLOR)==> Installing packages into GOPATH...$(NO_COLOR)"
	@go install ./...

vet:
	@echo "$(OK_COLOR)==> Running vet...$(NO_COLOR)"
	@go vet ./...

linter:
	@echo "$(OK_COLOR)==> Running vet...$(NO_COLOR)"
	@go lint ./...


generate:
	@echo "$(OK_COLOR)==> Generating files via go generate...$(NO_COLOR)"
	@go generate ./...

tags:
	@echo "$(OK_COLOR)==> Generating tags...$(NO_COLOR)"
	gotags -tag-relative=true -R=true -sort=true -f=".ctags" -fields=+l .

setup:
	@echo "$(OK_COLOR)==> Installing required components...$(NO_COLOR)"
	@go get -u $(GOFLAGS) golang.org/x/tools/cmd/vet
	@go get -u $(GOFLAGS) github.com/golang/lint/golint
	@go get -u $(GOFLAGS) github.com/campoy/jsonenums
	@go get -u $(GOFLAGS) github.com/davecgh/go-spew/spew
	@go get -u $(GOFLAGS) github.com/gogo/protobuf/proto
	@go get -u $(GOFLAGS) github.com/gogo/protobuf/protoc-gen-gogo
	@go get -u $(GOFLAGS) github.com/gogo/protobuf/gogoproto

clean:
	@echo "$(OK_COLOR)==> Cleaning...$(NO_COLOR)"
	@go clean $(GOFLAGS) -i ./...
