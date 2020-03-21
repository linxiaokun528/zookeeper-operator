module zookeeper-operator

go 1.13

require (
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.3.0
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/sirupsen/logrus v1.4.2
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	k8s.io/api v0.17.2
	k8s.io/apiextensions-apiserver v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v0.17.2
	k8s.io/component-base v0.17.2
	k8s.io/klog v1.0.0
	sigs.k8s.io/controller-runtime v0.5.1
)
