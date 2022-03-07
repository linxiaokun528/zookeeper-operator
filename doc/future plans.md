## Current Version
v0.2.0

## Future plan
### v0.2.1
- Add validation check, whether using `OpenAPI v3 schema` or `Admission Webhook`
- Use group v1 instead of v1beta to create CRD

### v0.3.0
Use `controller-runtime` to refactor this project

### v0.3.1
Use `kubebuilder` and `kustomizer` to manage this project

- Put CRD initialization outside of the operator
- Show more information/status in `kubectl get zookeepercluster`

### v0.3.2 (optional)
Something a pod is shown as running, but it's actually not working. (For example, when we watch a local IP)

So we need to use another readness probe. The current one we are using something does not work.

### v0.4.0
Add settings for PV and PVC


### v0.5.0
Add settings for observance

### v0.5.1
Add settings for JVM

- Consider using configmaps to store jvm and zoo.conf settings.

### v0.6.0
Add Unit Testing and Integration testing.

Setup a pipeline to automate these testings.

- Maybe write a Chaos Money to test the operator.

### Others
Deal with metrics in Prometeus

Maybe use a statefulset to manage the ZooKeeper clusters.

```
I am a starter for Go and Kubernetes, so I am developing this project for learning and fun.
Currently I am seeking a new job, and thus the development of this project is paused.
I will resume it after I find a new job.
```

