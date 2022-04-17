package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"zookeeper-operator/internal/config"
	"zookeeper-operator/pkg/k8sutil"
)

// logPredicate always returns true. It's just used to record the event that is received for log purpose.
// To make sure all event can be logged, this predicate should be the first predicate.
type logPredicate struct {
	level  klog.Level
	scheme *runtime.Scheme
	gvk    schema.GroupVersionKind
}

func NewLogPredicate(level klog.Level, gvk schema.GroupVersionKind) *logPredicate {
	return &logPredicate{level: level, gvk: gvk}
}

func (l *logPredicate) Create(event event.CreateEvent) bool {
	l.CheckTypeIfNecessary(event.Object)
	klog.V(l.level).Infof("Received a creation event of \"%s\" for \"%s\"",
		l.gvk, k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) Delete(event event.DeleteEvent) bool {
	l.CheckTypeIfNecessary(event.Object)
	klog.V(l.level).Infof("Received a deletion event of \"%s\" for \"%s\"",
		l.gvk, k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) Update(event event.UpdateEvent) bool {
	l.CheckTypeIfNecessary(event.ObjectOld)
	klog.V(l.level).Infof("Received an update event of \"%s\" for \"%s\"",
		l.gvk, k8sutil.GetNamespacedName(event.ObjectOld))
	return true
}

func (l *logPredicate) Generic(event event.GenericEvent) bool {
	l.CheckTypeIfNecessary(event.Object)
	klog.V(l.level).Infof("Received a generic event of \"%s\" for \"%s\"",
		l.gvk, k8sutil.GetNamespacedName(event.Object))
	return true
}

func (l *logPredicate) InjectScheme(scheme *runtime.Scheme) error {
	l.scheme = scheme
	return nil
}

func (l *logPredicate) CheckTypeIfNecessary(obj runtime.Object) {
	if klog.V(config.DebugLevel).Enabled() {
		l.CheckType(obj)
	}
}

func (l *logPredicate) CheckType(obj runtime.Object) {
	actualGVK := l.getGVKForObject(obj)
	if actualGVK != l.gvk {
		klog.Fatalf("type Error! Expected type is \"%s\" while actual type is \"%s\"", l.gvk, actualGVK)
	}
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
