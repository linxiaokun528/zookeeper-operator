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
	"reflect"
	"runtime"

	"github.com/go-zookeeper/zk"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/fatih/set.v0"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"zookeeper-operator/internal/config"
	zkcontroller "zookeeper-operator/internal/controller"
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

func init() {
	flag.StringVar(&namespace, "namespace", "", "The namespace this operator is running on")
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8088", "The address on which the HTTP server will listen to")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader elect")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-zkcluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-zkcluster.")

	klog.InitFlags(flag.CommandLine)
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
	zk.DefaultLogger = &loggerForGoZK{}
}

var clean context.CancelFunc = nil

func main() {
	defer logs.FlushLogs()
	defer klog.Flush()
	klog.Infof("zookeeper-operator Version: %v", version.Version)
	klog.Infof("Git SHA: %s", version.GitSHA)
	klog.Infof("Go Version: %s", runtime.Version())
	klog.Infof("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)

	if printVersion {
		os.Exit(0)
	}

	// todo: maybe remove this, or use k8s's way to do the metircs
	go func() {
		http.HandleFunc(probe.HTTPReadyzEndpoint, probe.ReadyzHandler)
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(listenAddr, nil)
	}()

	ctx := signals.SetupSignalHandler()
	ctx, clean = context.WithCancel(ctx)

	var err error
	mgr := getManagerOrDie()

	// todo: remove this when use kubebuilder to init crd
	k8sutil.InitCRDOrDie(mgr.GetClient(), ctx)

	logPredictForZkCluster := zkcontroller.NewLogPredicate(config.PredicateLogLevel, api.SchemeGroupVersionKind)
	logPredictForPod := zkcontroller.NewLogPredicate(config.PredicateLogLevel,
		v1.SchemeGroupVersion.WithKind(reflect.TypeOf(v1.Pod{}).Name()))
	podsToDelete := set.New(set.ThreadSafe)
	err = builder.
		ControllerManagedBy(mgr).
		For(&api.ZookeeperCluster{}, builder.WithPredicates(logPredictForZkCluster)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Watches(&source.Kind{Type: &v1.Pod{}},
			zkcontroller.NewZkPodEventHandler(ctx, podsToDelete),
			builder.WithPredicates(zkcontroller.NewZkPodPredicate(ctx), logPredictForPod)).
		Complete(zkcontroller.NewZookeeperReconciler(podsToDelete))
	dealWithError(err)

	err = mgr.Start(ctx)
	// todo: is err is"leader election lost", we may want to restart again
	// in early version, do we symply return when election lost?
	// We can use debug to test the election lost, because we can have
	// "failed to renew lease default/lshaokun-a01.vmware.com: timed out waiting for the condition"
	dealWithError(err)
}

func dealWithError(err error) {
	if err != nil {
		if clean != nil {
			clean()
		}
		panic(err)
	}
}

func getConfigOrDie() *rest.Config {
	restConfig, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		panic(err)
	}

	return restConfig
}

func getManagerOrDie() manager.Manager {
	var options *manager.Options
	if leaderElect {
		options = getManagerOptionsForClusterMode()
	} else {
		options = getManagerOptionsForStandaloneMode()
	}

	scheme := k8sruntime.NewScheme()
	err := clientgoscheme.AddToScheme(scheme)
	dealWithError(err)
	err = apiextensionsv1.AddToScheme(scheme)
	dealWithError(err)
	err = api.AddToScheme(scheme)
	dealWithError(err)
	options.Scheme = scheme

	// todo: remove this when use kubebuilder to init crd
	options.ClientDisableCacheFor = []client.Object{&apiextensionsv1.CustomResourceDefinition{}}
	options.Namespace = namespace
	options.Logger = klogr.New()

	mgr, err := manager.New(getConfigOrDie(), *options)
	if err != nil {
		panic(err)
	}
	return mgr
}

func getManagerOptionsForStandaloneMode() *manager.Options {
	return &manager.Options{
		LeaderElection: false,
	}
}

func getManagerOptionsForClusterMode() *manager.Options {
	id, err := os.Hostname()
	if err != nil {
		klog.Fatalf("failed to get hostname: %v", err)
	}
	return &manager.Options{
		LeaderElection:             true,
		LeaderElectionID:           id,
		LeaderElectionResourceLock: resourcelock.LeasesResourceLock,
		LeaderElectionNamespace:    namespace,
	}
}
