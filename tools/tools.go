//go:build tools

package tools

//go:generate go install github.com/mfridman/tparse@latest
//go:generate go install golang.org/x/tools/cmd/goimports@latest
//go:generate go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
