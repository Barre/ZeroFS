.PHONY: webui webui-install vm-image build build-release clean

webui-install:
	cd webui && npm ci

vm-image: webui-install
	webui/scripts/build-vm-image.sh

webui: webui-install vm-image
	cd webui && npm run build

build: webui
	cd zerofs && cargo build --features webui

build-release: webui
	cd zerofs && cargo build --profile release --features webui

clean:
	rm -rf webui/dist webui/node_modules webui/public/v86
	cd zerofs && cargo clean
