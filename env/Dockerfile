FROM golang:1.17 AS builder
ENV CGO_ENABLED 0
ENV GO111MODULE on
ENV GOFLAGS -mod=vendor
COPY . /go/src
WORKDIR /go/src
RUN go build -v -o datamanager .

# ===========================

FROM centos:centos8
COPY --from=builder /go/src/navigate /

WORKDIR /
ENV PARAMS=""
CMD ["sh","-c","/navigate $PARAMS  > /dev/null 2>&1"]