# DON'T USE THIS. WORK IN PROGRESS

![messageDB](docs/img/messagedb-logo.png "messageDB")

[![Build Status](https://travis-ci.org/messagedb/messagedb.svg)](https://travis-ci.org/messagedb/messagedb)



Installing Go
-------------
MessageDB requires Go 1.4.2 or greater.

At MessageDB we find gvm, a Go version manager, useful for installing Go. For instructions
on how to install it see [the gvm page on github](https://github.com/moovweb/gvm).

After installing gvm you can install and set the default go version by
running the following:

    gvm install go1.4.2
    gvm use go1.4.2 --default


Project structure
-----------------
First you need to setup the project structure:

    export GOPATH=$HOME/gocodez
    mkdir -p $GOPATH/src/github.com/messagedb
    cd $GOPATH/src/github.com/messagedb
    git clone git@github.com:messagedb/messagedb

You can add the line `export GOPATH=$HOME/gocodez` to your bash/zsh
file to be set for every shell instead of having to manually run it
everytime.

We have a pre commit hook to make sure code is formatted properly
and vetted before you commit any changes. We strongly recommend using the pre
commit hook to guard against accidentally committing unformatted
code. To use the pre-commit hook, run the following:

    cd $GOPATH/src/github.com/messagedb/messagedb
    cp .hooks/pre-commit .git/hooks/

In case the commit is rejected because it's not formatted you can run
the following to format the code:

```
go fmt ./...
go vet ./...
```

To install go vet, run the following command:
```
go get golang.org/x/tools/cmd/vet
```

To install jsonenums, run the following command:
```
go get github.com/campoy/jsonenums
```

To install support for protobuf generation, run the following command:
```
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
```

To install statik, run the following command:
```
go get github.com/rakyll/statik
```

Build and Test
-----

Make sure you have Go installed and the project structure as shown above. To then build the project, execute the following commands:

```bash
cd $GOPATH/src/github.com/messagedb
go get -u -f -t ./...
statik -src=./web/admin
go build ./...
```

To then install the binaries, run the following command. They can be found in `$GOPATH/bin`. Please note that the MessageDB binary is named `messagedbd`, not `messagedb`.

```bash
go install ./...
```

To set the version and commit flags during the build pass the following to the build command:

```bash
-ldflags="-X main.version $VERSION -X main.commit $COMMIT"
```

where `$VERSION` is the version, and `$COMMIT` is the git commit hash.

To run the tests, execute the following command:

```bash
cd $GOPATH/src/github.com/messagedb/messagedb
go test -v ./...

# run tests that match some pattern
go test -run=TestDatabase . -v

# run tests and show coverage
go test -coverprofile /tmp/cover . && go tool cover -html /tmp/cover
```

To install go cover, run the following command:
```
go get golang.org/x/tools/cmd/cover
```
