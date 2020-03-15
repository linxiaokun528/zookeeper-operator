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
	apiextensionsclientv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"net/http"
	"os"
	"runtime"
	"time"
	api "zookeeper-operator/apis/zookeeper/v1alpha1"
	"zookeeper-operator/client"
	"zookeeper-operator/util/k8sutil"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"zookeeper-operator/controller"
	"zookeeper-operator/util/constants"
	"zookeeper-operator/util/probe"
	"zookeeper-operator/version"

	"github.com/samuel/go-zookeeper/zk"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
)

var (
	printVersion bool
	leaderElect  bool

	masterURL  string
	kubeconfig string

	listenAddr string
)

func init() {
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8080", "The address on which the HTTP server will listen to")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader elect")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-zkcluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-zkcluster.")

	flag.Parse()
}

type emptyLogger struct{}

func (e *emptyLogger) Printf(string, ...interface{}) {}

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()
	zk.DefaultLogger = &emptyLogger{}

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
	ctx := context.TODO()

	if !leaderElect {
		zkController.Run(ctx)
	} else {
		namespace := getEnv(constants.EnvOperatorPodNamespace)
		name := getEnv(constants.EnvOperatorPodName)

		id, err := os.Hostname()
		if err != nil {
			klog.Fatalf("failed to get hostname: %v", err)
		}

		rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
			namespace,
			"zookeeper-operator",
			nil,
			cli.KubernetesInterface().CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: createRecorder(cli.KubernetesInterface(), name, namespace),
			})
		if err != nil {
			klog.Fatalf("error creating lock: %v", err)
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:          rl,
			LeaseDuration: 15 * time.Second,
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

func getEnv(name string) string {
	value := os.Getenv(name)
	if len(value) == 0 {
		klog.Fatalf("must set env (%s)", name)
	}

	return value
}

func createRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	// TODO: what does kubecli.CoreV1().RESTClient()).Events(namespace) do?
	// When an event happend in leader election, will the EventRecorder send an event to k8s?
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}

// TODO: consider manage CRD outside of operator. If so, refactor client.Client: use client.CRClient to replace
// client.Client. We won't need CRDClient anymore.
func initCRDOrDie(client apiextensionsclientv1.CustomResourceDefinitionInterface) {
	err := initCRD(client)
	if err != nil {
		panic(err)
	}
}

func initCRD(client apiextensionsclientv1.CustomResourceDefinitionInterface) error {
	crd := k8sutil.NewCRD(client, api.ZookeeperClusterCRDName, api.ZookeeperClusterResourceKind,
		api.ZookeeperClusterResourcePlural, "zookeeper")
	return crd.CreateAndWait()
}
