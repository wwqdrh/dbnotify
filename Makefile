godist:
	cd web \
	&& npm install && npm run build \
	&& cd ../ \
	&& go-bindata -o=./router/asset.go -pkg=router ./bdlog/... \
	&& rm -rf ./bdlog

protoc:
	protoc -I common/pqstream/proto/ common/pqstream/proto/pqstream.proto --go_out=plugins=grpc:common/pqstream/proto