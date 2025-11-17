
rm go.mod go.sum
go clean 
go clean -modcache
go mod init  github.com/glenn-edgar/knowledge_base/kb_modules/kb_go/kb_memory
go mod tidy
