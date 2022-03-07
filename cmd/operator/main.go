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
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"zookeeper-operator/internal/util/probe"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	"zookeeper-operator/internal/client"
	"zookeeper-operator/internal/controller"
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

const ComponentName = "zookeeper-operator"

func init() {
	flag.StringVar(&namespace, "namespace", "", "The namespace this operator is running on")
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8080", "The address on which the HTTP server will listen to")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader elect")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-zkcluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-zkcluster.")

	flag.Parse()
	if namespace == "" {
		panic(fmt.Errorf("option \"namespace\" can't be empty"))
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

	// maybe remove this
	go func() {
		http.HandleFunc(probe.HTTPReadyzEndpoint, probe.ReadyzHandler)
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(listenAddr, nil)
	}()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go handleSignal(ctx, cancel)

	cli := client.NewClientOrDie(ctx, masterURL, kubeconfig)
	initCRDOrDie(cli.GetCRDClient())

	if !leaderElect {
		runControllerOnStandaloneMode(ctx, cli) // will hang here
	} else {
		runControllerOnClusterMode(ctx, cli) // will hang here
	}
}

func handleSignal(ctx context.Context, cancelFunc context.CancelFunc) {
	exitSignals := []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT}
	sig := make(chan os.Signal, len(exitSignals))
	signal.Notify(sig, exitSignals...)

	klog.Infof("signal waiting...")
	select {
	case sign := <-sig:
		klog.Infof("Received singal %s...", sign)
	case <-ctx.Done():
	}

	close(sig)
	cancelFunc()
}

func runControllerOnStandaloneMode(ctx context.Context, cli client.Client) {
	zkController := controller.New(cli)
	zkController.Run(ctx)
}

func runControllerOnClusterMode(ctx context.Context, cli client.Client) {
	id, err := os.Hostname()
	if err != nil {
		klog.Fatalf("failed to get hostname: %v", err)
	}

	createLease(ctx, cli.KubernetesInterface())

	rl := resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      ComponentName,
		},
		Client: cli.KubernetesInterface().CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: createRecorder(cli.KubernetesInterface()),
		},
	}

	zkController := controller.New(cli)
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

func createRecorder(kubecli kubernetes.Interface) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: ComponentName})
}

func createLease(ctx context.Context, kubecli kubernetes.Interface) {
	// Set the labels so that it will be easier to clean all resources created by zookeeper-operator.
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      ComponentName,
			Labels:    map[string]string{"app": ComponentName},
		},
	}
	lease, err := kubecli.CoordinationV1().Leases(namespace).Create(ctx, lease, metav1.CreateOptions{})
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
	crd := newCRDForZookeeperCluster()
	if crd.Labels == nil {
		crd.Labels = map[string]string{}
	}
	crd.Labels["app"] = ComponentName
	return client.CreateAndWait(crd)
}

func newCRDForZookeeperCluster() *apiextensionsv1.CustomResourceDefinition {
	minSize := 1.0

	customResourceDefinition := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: api.Plural + "." + api.GroupName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: api.GroupName,
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    api.Version,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:     "object",
							Required: []string{"spec"},
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"spec": {
									Type:     "object",
									Required: []string{"size"},
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"size": {
											Type:    "integer",
											Minimum: &minSize,
										},
										"version": {
											Type: "string",
											// valid versions: "v0.0.0", "V3.2.3", "3.0.10", "1.1.1"
											// invalid versions: "v0.0.0.", "Vv3.2.3", "03.0.10", "1.1"
											Pattern: `^[vV]?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`,
											Default: &apiextensionsv1.JSON{
												Raw: []byte(strconv.Quote(api.DefaultZookeeperVersion)),
											},
										},
										"repository": {
											Type: "string",
											Default: &apiextensionsv1.JSON{
												Raw: []byte(strconv.Quote(api.DefaultRepository)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: api.Plural,
				Kind:   api.Kind,
			},
		},
	}
	if len(api.Short) != 0 {
		customResourceDefinition.Spec.Names.ShortNames = []string{api.Short}
	}

	return customResourceDefinition
}
