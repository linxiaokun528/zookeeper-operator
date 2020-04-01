package zkcluster

import (
	"k8s.io/klog"

	"zookeeper-operator/internal/util/zookeeperutil"
)

//func (c *Cluster) needReconfig() (bool, error) {
//	if c.zkCR.Status.Members.Running.Size() == 0 {
//		return false, nil
//	}
//	actualConfig, err := zookeeperutil.GetClusterConfig(c.zkCR.Status.Members.Running.GetClientHosts())
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
	klog.Infof("Reconfiguring zookeeper cluster %s: %v",
		c.zkCR.GetFullName(), c.zkCR.Status.Members.Running.GetClusterConfig())
	defer func() {
		if err == nil {
			klog.Infof("Zookeeper cluster %s are successfully reconfigured: %v",
				c.zkCR.GetFullName(), c.zkCR.Status.Members.Running.GetClusterConfig())
		}
	}()

	return zookeeperutil.ReconfigureCluster(c.zkCR.Status.Members.Running.GetClientHosts(),
		c.zkCR.Status.Members.Running.GetClusterConfig())
}
