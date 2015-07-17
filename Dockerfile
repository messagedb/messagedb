FROM golang:latest


COPY . /go/src/github.com/messagedb/messagedb

WORKDIR /go/src/github.com/messagedb/messagedb

RUN go get -u ./... \
    && go build -a ./...

EXPOSE 80
ENV PORT 80
ENV GIN_MODE release

CMD ./bin/messaged run
