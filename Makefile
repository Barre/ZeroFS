.PHONY: webui webui-install webui-proto-gen vm-image build build-release clean

webui-install:
	cd webui && npm ci

webui-proto-gen: webui-install
	cd webui && npx buf generate

vm-image: webui-install
	webui/scripts/build-vm-image.sh

webui: webui-install vm-image webui-proto-gen
	cd webui && npm run build

build: webui
	cd zerofs && cargo build --features webui

build-release: webui
	cd zerofs && cargo build --profile release --features webui

clean:
	rm -rf webui/dist webui/node_modules webui/public/v86
	cd zerofs && cargo clean
