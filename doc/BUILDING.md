Build the operator:
- `go list -m -json all`
- `./hack/k8s/debug/dlv-generator.sh`
- `./hack/k8s/codegen/update-generated.sh`
- `./hack/build/operator/build`
- `docker rmi -f zookeeper-operator:debug && docker build -t zookeeper-operator:debug -f ./hack/build/operator/Dockerfile .`