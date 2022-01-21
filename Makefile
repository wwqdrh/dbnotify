godist:
	cd web \
	&& npm install && npm run build \
	&& cd ../ \
	&& go-bindata -o=./server/common/asset.go -pkg=common ./bdlog/... \
	&& rm -rf ./bdlog
	