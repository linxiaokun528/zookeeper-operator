# Zookeeper operator

### Project status: alpha

## Overview

This project is forked and adapted from [Nuance-Mobility/zookeeper-operator][original-operator-url]

The Zookeeper operator manages zookeeper clusters deployed to [Kubernetes][k8s-home] and automates tasks related to operating a Zookeeper cluster.

- [Create and destroy](#create-and-destroy-a-zookeeper-cluster)
- [Resize](#resize-a-zookeeper-cluster)
- [Recover a member](#member-recovery)
- [Rolling upgrade](#upgrade-a-zookeeper-cluster)

## Requirements

- Kubernetes 1.15+
- Zookeeper 3.5.0+

## Install Zookeeper operator

Refer to [BUILDING][building-doc] to build zookeeper-operator image.

Create a deployment for Zookeeper operator:

```bash
$ kubectl create -f example/deployment.yaml
```

Zookeeper operator will automatically create a Kubernetes Custom Resource Definition (CRD):

```bash
$ kubectl get customresourcedefinitions
NAME                                              AGE
zookeeperclusters.zookeeper.database.apache.com   1m
```

## Uninstall Zookeeper operator

Note that the Zookeeper clusters managed by Zookeeper operator will **NOT** be deleted even if the operator is uninstalled.

This is an intentional design to prevent accidental operator failure from killing all the Zookeeper clusters.

To delete all clusters, delete all cluster CR objects before uninstalling the operator.

Clean up Zookeeper operator:

1. Delete the operator.

    ```bash
    $ kubectl delete -f example/deployment.yaml
    ```

2. Clean related resources.

    ```bash
    $ kubectl api-resources --verbs=get -o name | grep -v componentstatus | xargs -n 1 kubectl get  --show-kind --ignore-not-found -l app=zookeeper-operator -o name | xargs kubectl delete
    ```

## Create and destroy a Zookeeper cluster

Refer to [BUILDING][building-doc] to build zookeeper-instance image.

Create a Zookeeper cluster:
```bash
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

A 3 member Zookeeper cluster will be created.

```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-1       1/1       Running   0          1m
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
```

Destroy Zookeeper cluster:

```bash
$ kubectl delete -f example/example-zookeeper-cluster.yaml
```

## Resize a Zookeeper cluster

Create a Zookeeper cluster:

```
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```

In `example/example-zookeeper-cluster.yaml` the initial cluster size is 3.
Modify the file and change `size` from 3 to 5.

```
$ cat example/example-zookeeper-cluster.yaml
apiVersion: "zookeeper.database.apache.com/v1alpha1"
kind: "ZookeeperCluster"
metadata:
  name: "example-zookeeper-cluster"
spec:
  size: 5
  version: "3.5.7"
```

Apply the size change to the cluster CR:
```
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```
The Zookeeper cluster will scale to 5 members (5 pods):
```
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-1       1/1       Running   0          1m
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
example-zookeeper-cluster-5       1/1       Running   0          1m
```

Similarly we can decrease the size of cluster from 5 back to 3 by changing the size field again and reapplying the change.

```
$ cat example/example-zookeeper-cluster.yaml
apiVersion: "zookeeper.database.apache.com/v1alpha1"
kind: "ZookeeperCluster"
metadata:
  name: "example-zookeeper-cluster"
spec:
  size: 3
  version: "3.5.7"
```
```
$ kubectl apply -f example/example-zookeeper-cluster.yaml
```

We should see that Zookeeper cluster will eventually reduce to 3 pods:

```
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-5       1/1       Running   0          1m
```

## Member recovery

If the minority of Zookeeper members crash, the Zookeeper operator will automatically recover the failure.
Let's walk through in the following steps.

Create a Zookeeper cluster:

```
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

Wait until all three members are up. Simulate a member failure by deleting a pod:

```bash
$ kubectl delete pod example-zookeeper-cluster-1 --now
```

The Zookeeper operator will recover the failure by creating a new pod `example-zookeeper-cluster-4`:

```bash
$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
```

Destroy Zookeeper cluster:
```bash
$ kubectl delete -f example/example-zookeeper-cluster.yaml
```

## Zookeeper operator recovery

If the Zookeeper operator restarts, it can recover its previous state.
Let's walk through in the following steps.

```
$ kubectl create -f example/example-zookeeper-cluster.yaml
```

Wait until all three members are up. Then

```bash
$ kubectl delete -f example/deployment.yaml
deployment "zookeeper-operator" deleted

$ kubectl delete pod example-zookeeper-cluster-1 --now
pod "example-zookeeper-cluster-1" deleted
```

Then restart the Zookeeper operator. It should recover itself and the Zookeeper clusters it manages.

```bash
$ kubectl create -f example/deployment.yaml
deployment "zookeeper-operator" created

$ kubectl get pods
NAME                            READY     STATUS    RESTARTS   AGE
example-zookeeper-cluster-2       1/1       Running   0          1m
example-zookeeper-cluster-3       1/1       Running   0          1m
example-zookeeper-cluster-4       1/1       Running   0          1m
```


[k8s-home]: http://kubernetes.io
[original-operator-url]: https://github.com/Nuance-Mobility/zookeeper-operator
[building-doc]: ./doc/BUILDING.md