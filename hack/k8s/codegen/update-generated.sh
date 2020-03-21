#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

$GOPATH/pkg/mod/k8s.io/code-generator\@v0.17.2/generate-groups.sh all zookeeper-operator/pkg/client zookeeper-operator/pkg/apis zookeeper:v1alpha1 --go-header-file hack/k8s/codegen/boilerplate.go.txt --output-base=..

# 若用vendor模式，则为：
# vendor/k8s.io/code-generator/generate-groups.sh all pkg/generated zookeeper-operator/pkg/apis zookeeper:v1alpha1 --go-header-file hack/k8s/codegen/boilerplate.go.txt --output-base=.

#/Users/jelin/go/pkg/mod/k8s.io/code-generator\@v0.17.2/generate-groups.sh all zookeeper-operator/generated zookeeper-operator/apis zookeeper:v1alpha1 --go-header-file hack/k8s/codegen/boilerplate.go.txt --output-base=..

# 下面这样是错的。会导致生成./zookeeper-operator/apis/zookeeper-operator/vialpha1/zz_generated.deepcopy.go
# 以及 generated文件夹下的许多文件import 都为 "generated/clientset/versioned/typed/zookeeper/v1alpha1"。但实际应改为"zookeeper-operator/generated/clientset/versioned/typed/zookeeper/v1alpha1"
# /Users/jelin/go/pkg/mod/k8s.io/code-generator\@v0.17.2/generate-groups.sh all generated zookeeper-operator/apis zookeeper:v1alpha1 --go-header-file hack/k8s/codegen/boilerplate.go.txt --output-base=.