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
	"zookeeper-operator/client"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"zookeeper-operator/chaos"
	"zookeeper-operator/controller"
	"zookeeper-operator/util/constants"
	"zookeeper-operator/util/probe"
	"zookeeper-operator/version"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

var (
	namespace  string
	name       string
	listenAddr string
	gcInterval time.Duration
	masterURL  string
	kubeconfig string
	chaosLevel int

	printVersion bool

	createCRD bool

	clusterWide bool
	leaderElect bool

	cli client.Client
)

func init() {
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8080", "The address on which the HTTP server will listen to")
	// chaos level will be removed once we have a formal tool to inject failures.
	flag.IntVar(&chaosLevel, "chaos-level", -1, "DO NOT USE IN PRODUCTION - level of chaos injected into the zookeeper clusters created by the operator.")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&createCRD, "create-crd", true, "The operator will not create the ZookeeperCluster CRD when this flag is set to false.")
	flag.DurationVar(&gcInterval, "gc-interval", 10*time.Minute, "GC interval")
	flag.BoolVar(&clusterWide, "cluster-wide", false, "Enable operator to watch clusters in all namespaces")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader elect")

	flag.Parse()
}

func main() {
	namespace = getEnv(constants.EnvOperatorPodNamespace)
	name = getEnv(constants.EnvOperatorPodName)

	if printVersion {
		fmt.Println("zookeeper-operator Version:", version.Version)
		fmt.Println("Git SHA:", version.GitSHA)
		fmt.Println("Go Version:", runtime.Version())
		fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	logrus.Infof("zookeeper-operator Version: %v", version.Version)
	logrus.Infof("Git SHA: %s", version.GitSHA)
	logrus.Infof("Go Version: %s", runtime.Version())
	logrus.Infof("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)

	id, err := os.Hostname()
	if err != nil {
		logrus.Fatalf("failed to get hostname: %v", err)
	}

	cli = client.MustNewClient(masterURL, kubeconfig)

	http.HandleFunc(probe.HTTPReadyzEndpoint, probe.ReadyzHandler)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(listenAddr, nil)

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
		logrus.Fatalf("error creating lock: %v", err)
	}

	if leaderElect {
		// TODO: Understand the difference between context.TODO() and context.background()： https://www.cnblogs.com/zhangboyu/p/7456606.html
		leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
			Lock:          rl,
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: run,
				OnStoppedLeading: func() {
					logrus.Fatalf("leader election lost")
				},
			},
		})
	} else {
		ctx := context.TODO()
		run(ctx)
	}

	panic("unreachable")
}

func getEnv(name string) string {
	value := os.Getenv(name)
	if len(value) == 0 {
		logrus.Fatalf("must set env (%s)", name)
	}

	return value
}

func run(ctx context.Context) {
	cfg := newControllerConfig()

	// startChaos(ctx, cfg.KubeCli, cfg.Namespace, chaosLevel)

	c := controller.New(cfg, cli, ctx)
	c.Start()
}

func newControllerConfig() controller.Config {
	cfg := controller.Config{
		Namespace:   namespace,
		ClusterWide: clusterWide,
		CreateCRD:   createCRD,
		MasterURL:   masterURL,
		Kubeconfig:  kubeconfig,
	}

	return cfg
}

func startChaos(ctx context.Context, kubecli kubernetes.Interface, ns string, chaosLevel int) {
	m := chaos.NewMonkeys(kubecli)
	ls := labels.SelectorFromSet(map[string]string{"app": "zookeeper"})

	switch chaosLevel {
	case 1:
		logrus.Info("chaos level = 1: randomly kill one zookeeper pod every 30 seconds at 50%")
		c := &chaos.CrashConfig{
			Namespace: ns,
			Selector:  ls,

			KillRate:        rate.Every(30 * time.Second),
			KillProbability: 0.5,
			KillMax:         1,
		}
		go func() {
			time.Sleep(60 * time.Second) // don't start until quorum up
			m.CrushPods(ctx, c)
		}()

	case 2:
		logrus.Info("chaos level = 2: randomly kill at most five zookeeper pods every 30 seconds at 50%")
		c := &chaos.CrashConfig{
			Namespace: ns,
			Selector:  ls,

			KillRate:        rate.Every(30 * time.Second),
			KillProbability: 0.5,
			KillMax:         5,
		}

		go m.CrushPods(ctx, c)

	default:
	}
}

func createRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: what does kubecli.CoreV1().RESTClient()).Events(namespace) do?
	// When an event happend in leader election, will the EventRecorder send an event to k8s?
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}
