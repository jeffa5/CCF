BUILD=build
INSTALL_PREFIX=$(BUILD)/myopt
GLOBAL_INSTALL_PREFIX=/opt/ccf

CPP_INCLUDES=$(wildcard include/ccf/**/*.cpp)
H_INCLUDES=$(wildcard include/ccf/**/*.h)

IMAGE_NAME=ghcr.io/jeffa5/ccf

.PHONY: build-virtual
build-virtual:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=virtual -DCMAKE_INSTALL_PREFIX=$(abspath $(INSTALL_PREFIX))_virtual -DVERBOSE_LOGGING=OFF ..
	cd $(BUILD) && ninja

.PHONY: build-virtual-verbose
build-virtual-verbose:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=virtual -DCMAKE_INSTALL_PREFIX=$(abspath $(INSTALL_PREFIX))_virtual -DVERBOSE_LOGGING=ON ..
	cd $(BUILD) && ninja

.PHONY: build-virtual-global
build-virtual-global:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=virtual -DCMAKE_INSTALL_PREFIX=$(GLOBAL_INSTALL_PREFIX)_virtual -DVERBOSE_LOGGING=OFF ..
	cd $(BUILD) && ninja

.PHONY: build-sgx-global
build-sgx-global:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=sgx -DCMAKE_INSTALL_PREFIX=$(GLOBAL_INSTALL_PREFIX)_sgx -DVERBOSE_LOGGING=OFF -DUNSAFE_VERSION=OFF ..
	cd $(BUILD) && ninja

.PHONY: build-snp-global
build-snp-global:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=snp -DCMAKE_INSTALL_PREFIX=$(GLOBAL_INSTALL_PREFIX)_snp -DVERBOSE_LOGGING=OFF -DUNSAFE_VERSION=OFF ..
	cd $(BUILD) && ninja

.PHONY: build-virtual-global-verbose
build-virtual-global-verbose:
	mkdir -p $(BUILD)
	cd $(BUILD) && cmake -GNinja -DCOMPILE_TARGET=virtual -DCMAKE_INSTALL_PREFIX=$(GLOBAL_INSTALL_PREFIX)_virtual -DVERBOSE_LOGGING=ON ..
	cd $(BUILD) && ninja

.PHONY: build-docker-virtual
build-docker-virtual:
	docker build -t $(IMAGE_NAME):lskv-virtual -f docker/app_dev . --build-arg="clang_version=15" --build-arg="platform=virtual"

.PHONY: build-docker-sgx
build-docker-sgx:
	docker build -t $(IMAGE_NAME):lskv-sgx -f docker/app_dev . --build-arg="clang_version=10" --build-arg="platform=sgx"

.PHONY: build-docker
build-docker: build-docker-virtual build-docker-sgx

.PHONY: push-docker-virtual
push-docker-virtual:
	docker push $(IMAGE_NAME):lskv-virtual

.PHONY: push-docker-sgx
push-docker-sgx:
	docker push $(IMAGE_NAME):lskv-sgx

.PHONY: push-docker
push-docker: push-docker-sgx push-docker-virtual

.PHONY: install-virtual
install-virtual: build-virtual
	cd $(BUILD) && ninja install

.PHONY: install-virtual-global
install-virtual-global: build-virtual-global
	cd $(BUILD) && sudo ninja install

.PHONY: install-sgx-global
install-sgx-global: build-sgx-global
	cd $(BUILD) && sudo ninja install

.PHONY: install-virtual-global-verbose
install-virtual-global-verbose: build-virtual-global-verbose
	cd $(BUILD) && sudo ninja install

.PHONY: run-sandbox
run-sandbox: build-virtual
	cd $(BUILD) && ../tests/sandbox/sandbox.sh

.PHONY: run-sandbox-cpp-logging
run-sandbox-cpp-logging: build-virtual
	cd $(BUILD) && ../tests/sandbox/sandbox.sh -p samples/apps/logging/liblogging

.PHONY: test-virtual
test-virtual: build-virtual
	cd $(BUILD) && ./tests.sh

.PHONY: clean
clean:
	rm -rf $(INSTALL_PREFIX) $(BUILD) workspace

cpplint: $(CPP_INCLUDES) $(H_INCLUDES)
	cpplint --filter=-whitespace/braces,-whitespace/indent,-whitespace/comments,-whitespace/newline,-build/include_order,-build/include_subdir,-runtime/references,-runtime/indentation_namespace $(CPP_INCLUDES) $(H_INCLUDES)
