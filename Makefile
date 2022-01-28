godist:
	cd web \
	&& npm install && npm run build \
	&& cd ../ \
	&& go-bindata -o=./router/asset.go -pkg=router ./bdlog/... \
	&& rm -rf ./bdlog
	