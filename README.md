# ZooKeeper Operator

### Project status: alpha

## Overview

This project is forked and adapted from [Nuance-Mobility/zookeeper-operator][original-Operator-url]

The ZooKeeper Operator manages zookeeper clusters deployed to [Kubernetes][k8s-home] and automates tasks related to operating a ZooKeeper cluster.

- [Create and destroy](#Create-and-Destroy-a-ZooKeeper-Cluster)
- [Resize](#resize-a-ZooKeeper-cluster)
- [Recover](#Recover-Members)
- [Rolling upgrade](#Rolling-Upgrade-ZooKeeper-Cluster)

## Requirements

- Kubernetes 1.15+
- ZooKeeper 3.5.0+

## Install ZooKeeper Operator

Refer to [BUILDING][building-doc] to build zookeeper-operator images.

Create a deployment for ZooKeeper Operator:

```bash
$ kubectl create -f example/deployment.yaml
```

ZooKeeper Operator will automatically create a Kubernetes Custom Resource Definition (CRD):

```bash
$ kubectl get customresourcedefinitions
NAME                                              AGE
zookeeperclusters.zookeeper.database.apache.com   1m
```

## Uninstall ZooKeeper Operator

Note that the ZooKeeper clusters managed by ZooKeeper Operator will **NOT** be deleted even if the Operator is uninstalled.

This is an intentional design to prevent accidental Operator failure from killing all the ZooKeeper clusters.

To delete all clusters, delete all cluster CR objects before uninstalling the Operator.

Clean up ZooKeeper Operator:

1. Delete the Operator.

    ```bash
    $ kubectl delete -f example/deployment.yaml
    ```

2. Clean related resources.

    ```bash
    $ kubectl api-resources --verbs=get -o name | grep -v componentstatus | xargs -n 1 kubectl get  --show-kind --ignore-not-found -l app=zookeeper-operator -o name | xargs kubectl delete
    ```

## Create and Destroy a ZooKeeper Cluster

Refer to [BUILDING][building-doc] to build zookeeper-instance images.

Create a ZooKeeper cluster:
```bash
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

A 3 member ZooKeeper cluster will be created.

```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-1       1/1       Running   0          1m
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
```

Destroy ZooKeeper cluster:

```bash
$ kubectl delete -f example/example-zookeeper-cluster.yaml
```

## Resize a ZooKeeper cluster

Create a ZooKeeper cluster:

```bash
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```

In `example/example-zookeeper-cluster.yaml` the initial cluster size is 3.
Modify the file and change `size` from 3 to 5.

```bash
$ cat example/example-zookeeper-cluster.yaml
apiVersion: "zookeeper.database.apache.com/v1alpha1"
kind: "ZooKeeperCluster"
metadata:
  name: "example-zookeeper-cluster"
spec:
  size: 5
  version: "3.5.7"
```

Apply the size change to the cluster CR:
```bash
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```

The ZooKeeper cluster will scale to 5 members (5 pods):
```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-1       1/1       Running   0          1m
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
example-zookeeper-cluster-5       1/1       Running   0          1m
```

Similarly we can decrease the size of cluster from 5 back to 3 by changing the size field again and reapplying the change.

```bash
$ cat example/example-zookeeper-cluster.yaml
apiVersion: "zookeeper.database.apache.com/v1alpha1"
kind: "ZooKeeperCluster"
metadata:
  name: "example-zookeeper-cluster"
spec:
  size: 3
  version: "3.5.7"
```
```bash
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```

We should see that ZooKeeper cluster will eventually reduce to 3 pods:

```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-5       1/1       Running   0          1m
```

## Recover Members 

If the minority of ZooKeeper members crash, the ZooKeeper Operator will automatically recover the failure.
Let's walk through in the following steps.

Create a ZooKeeper cluster:

```bash
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

Wait until all three members are up. Simulate a member failure by deleting a pod:

```bash
$ kubectl delete pod example-zookeeper-cluster-1 --now
```

The ZooKeeper Operator will recover the failure by creating a new pod `example-zookeeper-cluster-4`:

```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
```

Destroy ZooKeeper cluster:
```bash
$ kubectl delete -f example/example-zookeeper-cluster.yaml
```

## Recover ZooKeeper Operator

If the ZooKeeper Operator restarts, it can recover its previous state.
Let's walk through in the following steps.

```bash
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

Wait until all three members are up. Then

```bash
$ kubectl delete -f example/deployment.yaml
deployment "zookeeper-operator" deleted

$ kubectl delete pod example-zookeeper-cluster-1 --now
pod "example-zookeeper-cluster-1" deleted
```

Then restart the ZooKeeper Operator. It should recover itself and the ZooKeeper clusters it manages.

```bash
$ kubectl create -f example/deployment.yaml
deployment "zookeeper-operator" created

$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
```

## Rolling Upgrade ZooKeeper Cluster

Refer to [BUILDING][building-doc] to build zookeeper-instance images.

Prerequisition: we need to have an old and a new zookeeper-instance image.
```bash
$ docker images| grep zookeeper-instance
 zookeeper-instance        3.5.8         09d22b7c9006        About a minute ago        245MB
 zookeeper-instance        3.5.7         89fb69febb56        7 weeks ago               245MB
```
Create a ZooKeeper cluster:
```bash
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```
```bash
$ kubectl describe pod/example-zookeeper-cluster-0| grep Image| grep zookeeper-instance
Image:          zookeeper-instance:3.5.7
```

In `example/example-zookeeper-cluster.yaml` the initial zookeeper version is "3.5.7".
Modify the file and change `version` from "3.5.7" to "3.5.8".

```bash
$ cat example/example-zookeeper-cluster.yaml
apiVersion: "zookeeper.database.apache.com/v1alpha1"
kind: "ZooKeeperCluster"
metadata:
  name: "example-zookeeper-cluster"
spec:
  size: 3
  version: "3.5.8"
```

Apply the change to the cluster CR:
```bash
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```
The ZooKeeper cluster will be upgraded to version "3.5.8":
```bash
$ kubectl describe pod/example-zookeeper-cluster-0| grep Image| grep zookeeper-instance
Image:          zookeeper-instance:3.5.8
```

## Future Plans
Refer to [future-plans][future-plans]


[k8s-home]: http://kubernetes.io
[original-Operator-url]: https://github.com/Nuance-Mobility/zookeeper-operator
[building-doc]: ./doc/BUILDING.md
[future-plans]: ./doc/future%20plans.md