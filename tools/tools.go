//go:build tools

package tools

//go:generate go install github.com/mfridman/tparse@v0.11.1
//go:generate go install golang.org/x/tools/cmd/goimports@latest
//go:generate go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.47.2
