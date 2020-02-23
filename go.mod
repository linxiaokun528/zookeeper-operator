module zookeeper-operator

go 1.13

require (
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.3.0
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/sirupsen/logrus v1.4.2
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	k8s.io/api v0.0.0-20190913080256-21721929cffa
	k8s.io/apiextensions-apiserver v0.0.0-20190913084637-b88784537d9e
	k8s.io/apimachinery v0.0.0-20190913075812-e119e5e154b6
	k8s.io/client-go v0.0.0-20190913080822-26b1e9b52936
)
