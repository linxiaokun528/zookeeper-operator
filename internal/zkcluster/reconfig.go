package zkcluster

import (
	"k8s.io/klog/v2"

	"zookeeper-operator/internal/util/zookeeper"
)

//func (c *Cluster) needReconfig() (bool, error) {
//	if c.zkCR.Status.Members.Running.Size() == 0 {
//		return false, nil
//	}
//	actualConfig, err := zookeeper.GetClusterConfig(c.zkCR.Status.Members.Running.GetClientHosts())
//	if err != nil {
//		c.logger.Info("Failed to get configure from zookeeper: %v",
//			c.zkCR.Status.Members.Running.GetClientHosts())
//		return false, err
//	}
//	expectedConfig := c.zkCR.Status.Members.Running.GetClusterConfig()
//
//	sort.Strings(actualConfig)
//	sort.Strings(expectedConfig)
//
//	return !reflect.DeepEqual(expectedConfig, actualConfig), nil
//}

func (c *Cluster) reconfig() (err error) {
	all := c.zkCR.Status.Members.Running.Copy()
	all.Update(&c.zkCR.Status.Members.Ready)

	klog.Infof("Reconfiguring zookeeper cluster %s: %v",
		c.zkCR.GetNamespacedName(), all.GetClusterConfig())
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %s are successfully reconfigured: %v",
				c.zkCR.GetNamespacedName(), all.GetClusterConfig())
		}
	}()

	return zookeeper.ReconfigureCluster(all.GetClientHosts(), all.GetClusterConfig())
}
