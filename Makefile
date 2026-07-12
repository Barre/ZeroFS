.PHONY: webui webui-install webui-proto-gen webui-wasm vm-image build build-release clean

webui-install:
	cd webui && npm ci

webui-proto-gen: webui-install
	cd webui && npx buf generate

webui-wasm:
	wasm-pack build zerofs/zerofs-client-web --target web --release --out-dir $(CURDIR)/webui/src/generated/zerofs-client --out-name zerofs_client

vm-image: webui-install
	webui/scripts/build-vm-image.sh

webui: webui-install vm-image webui-proto-gen webui-wasm
	cd webui && npm run build

build: webui
	cd zerofs && cargo build --features webui

build-release: webui
	cd zerofs && cargo build --profile release --features webui

clean:
	rm -rf webui/dist webui/node_modules webui/public/v86 webui/src/generated/zerofs-client
	cd zerofs && cargo clean
