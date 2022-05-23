package org.apache.zookeeper.jmx;

/**
 * 暴露的MBean信息接口
 * Zookeeper MBean info interface. MBeanRegistry uses the interface to generate
 * JMX object name.
 */
public interface ZKMBeanInfo {

    /**
     * 获取MBean识别名
     *
     * @return a string identifying the MBean
     */
    String getName();

    /**
     * 是否不注册到MBean Server中
     * If isHidden returns true, the MBean won't be registered with MBean server,
     * and thus won't be available for management tools. Used for grouping MBeans.
     *
     * @return true if the MBean is hidden.
     */
    boolean isHidden();

}
