all: build

TAG?=dev
FLAGS=
ENVVAR=
GOOS?=linux

deps:
	go get github.com/tools/godep

build: clean deps
	$(ENVVAR) GOOS=$(GOOS) godep go build ./...
	$(ENVVAR) GOOS=$(GOOS) godep go build -o cluster-autoscaler

test-unit: clean deps build
	$(ENVVAR) godep go test --test.short -race ./... $(FLAGS)

release: build
ifndef REGISTRY
	ERR = $(error REGISTRY is undefined)
	$(ERR)
endif
	docker build --pull -t ${REGISTRY}/cluster-autoscaler:${TAG} .
	gcloud docker push ${REGISTRY}/cluster-autoscaler:${TAG}

clean:
	rm -f cluster-autoscaler

format:
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -d {} + | tee /dev/stderr)" || \
    test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -w {} + | tee /dev/stderr)"

push: build
	docker build --tag=bhvrops/cluster-autoscaler .
	docker push bhvrops/cluster-autoscaler
	sleep 5
	kubectl delete pod $(shell kubectl get pods  | grep ^cluster-autoscaler | cut -d' ' -f1)

log:
	test 0 -eq 0; i=0; while [ $$? -eq 0 ]; do if [ $$i -gt 0 ]; then echo "STARTING"; sleep 10; fi; i=1; kubectl logs $$(kubectl get pods  | grep ^cluster-autoscaler | cut -d' ' -f1) -f; done

.PHONY: all deps build test-unit clean format release push
