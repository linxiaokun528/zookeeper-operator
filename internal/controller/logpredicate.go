package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"zookeeper-operator/pkg/k8sutil"
)

// logPredicate always returns true. It's just used to record the event that is received for log purpose.
// To make sure all event can be logged, this predicate should be the first predicate.
type logPredicate struct {
	level  klog.Level
	scheme *runtime.Scheme
}

func NewLogPredicate(level klog.Level) *logPredicate {
	return &logPredicate{level: level}
}

func (l *logPredicate) Create(event event.CreateEvent) bool {
	klog.V(l.level).Infof("Receive a creation event of \"%s\" for \"%s\"",
		l.getGVKForObject(event.Object), k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) Delete(event event.DeleteEvent) bool {
	klog.V(l.level).Infof("Receive a deletion event of \"%s\" for \"%s\"",
		l.getGVKForObject(event.Object), k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) Update(event event.UpdateEvent) bool {
	klog.V(l.level).Infof("Receive an update event of \"%s\" for \"%s\"",
		l.getGVKForObject(event.ObjectOld), k8sutil.GetNamespacedName(event.ObjectOld))
	return true
}

func (l *logPredicate) Generic(event event.GenericEvent) bool {
	klog.V(l.level).Infof("Receive a generic event of \"%s\" for \"%s\"",
		l.getGVKForObject(event.Object), k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) InjectScheme(scheme *runtime.Scheme) error {
	l.scheme = scheme
	return nil
}

func (l *logPredicate) getGVKForObject(obj runtime.Object) schema.GroupVersionKind {
	if !obj.GetObjectKind().GroupVersionKind().Empty() {
		return obj.GetObjectKind().GroupVersionKind()
	}
	gvk, err := apiutil.GVKForObject(obj, l.scheme)
	if err != nil {
		panic(err)
	}
	return gvk
}
