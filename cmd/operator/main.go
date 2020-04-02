// Copyright 2018 The zookeeper-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samuel/go-zookeeper/zk"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/logs"
	"k8s.io/klog"

	"zookeeper-operator/internal/client"
	"zookeeper-operator/internal/controller"
	"zookeeper-operator/internal/util/probe"
	"zookeeper-operator/internal/version"
	api "zookeeper-operator/pkg/apis/zookeeper/v1alpha1"
	"zookeeper-operator/pkg/k8sutil"
)

var (
	printVersion bool
	leaderElect  bool

	masterURL  string
	kubeconfig string
	namespace  string

	listenAddr string
)

const COMPONENT_NAME = "zookeeper-operator"

func init() {
	flag.StringVar(&namespace, "namespace", "", "The namespace this operator is running on")
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8080", "The address on which the HTTP server will listen to")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader elect")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-zkcluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-zkcluster.")

	flag.Parse()
	if namespace == "" {
		panic(fmt.Errorf("Option \"namespace\" can't be empty!"))
	}
}

type loggerForGoZK struct{}

func (e *loggerForGoZK) Printf(format string, args ...interface{}) {
	klog.V(4).Infof(format, args...)
}

func init() {
	logs.InitLogs()
	defer logs.FlushLogs()
	zk.DefaultLogger = &loggerForGoZK{}
}

func main() {
	klog.Infof("zookeeper-operator Version: %v", version.Version)
	klog.Infof("Git SHA: %s", version.GitSHA)
	klog.Infof("Go Version: %s", runtime.Version())
	klog.Infof("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)

	if printVersion {
		os.Exit(0)
	}

	cli := client.NewClientOrDie(masterURL, kubeconfig)
	initCRDOrDie(cli.GetCRDClient())

	http.HandleFunc(probe.HTTPReadyzEndpoint, probe.ReadyzHandler)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(listenAddr, nil)

	zkController := controller.New(cli)
	ctx := context.Background()

	if !leaderElect {
		zkController.Run(ctx)
	} else {
		id, err := os.Hostname()
		if err != nil {
			klog.Fatalf("failed to get hostname: %v", err)
		}

		createLease(cli.KubernetesInterface())

		rl := resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      COMPONENT_NAME,
			},
			Client: cli.KubernetesInterface().CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: createRecorder(cli.KubernetesInterface()),
			},
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:          &rl,
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: zkController.Run,
				OnStoppedLeading: func() {
					klog.Fatalf("leader election lost")
				},
			},
		})
	}

	panic("unreachable")
}

func createRecorder(kubecli kubernetes.Interface) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: COMPONENT_NAME})
}

func createLease(kubecli kubernetes.Interface) {
	// Set the labels so that it will be easier to clean all resources created by zookeeper-operator.
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      COMPONENT_NAME,
			Labels:    map[string]string{"app": COMPONENT_NAME},
		},
	}
	lease, err := kubecli.CoordinationV1().Leases(namespace).Create(lease)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		panic(err)
	}
}

// TODO: consider manage CRDClient outside of operator. If so, refactor zkclient.Client: use zkclient.CRClient to replace
// zkclient.Client. We won't need CRDClient anymore.
func initCRDOrDie(client k8sutil.CRDClient) {
	err := initCRD(client)
	if err != nil {
		panic(err)
	}
}

func initCRD(client k8sutil.CRDClient) error {
	// Set the labels so that it will be easier to clean all resources created by zookeeper-operator.
	crd := k8sutil.NewCRD(apiextensionsv1.NamespaceScoped, api.SchemeGroupVersionKind, api.Plural, api.Short)
	if crd.Labels == nil {
		crd.Labels = map[string]string{}
	}
	crd.Labels["app"] = COMPONENT_NAME
	return client.CreateAndWait(crd)
}
