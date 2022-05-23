package org.apache.zookeeper;

/**
 * 配额管理
 * 节点增加配额后会在/zookeeper/quota下创建同名节点
 * 节点下有限制与节点状态子节点用于管理节点配额
 * this class manages quotas
 * and has many other utils
 * for quota
 */
public class Quotas {

    /**
     * 根路径
     * the zookeeper nodes that acts as the management and status node
     **/
    public static final String procZookeeper = "/zookeeper";

    /**
     * 配额路径
     * the zookeeper quota node that acts as the quota
     * management node for zookeeper
     */
    public static final String quotaZookeeper = "/zookeeper/quota";

    /**
     * 限制文件名
     * the limit node that has the limit of
     * a subtree
     */
    public static final String limitNode = "zookeeper_limits";

    /**
     * 当前状态文件名
     * the stat node that monitors the limit of
     * a subtree.
     */
    public static final String statNode = "zookeeper_stats";

    /**
     * return the quota path associated with this
     * prefix
     *
     * @param path the actual path in zookeeper.
     * @return the quota path
     */
    public static String quotaPath(String path) {
        return quotaZookeeper + path;
    }

    /**
     * return the limit quota path associated with this
     * prefix
     *
     * @param path the actual path in zookeeper.
     * @return the limit quota path
     */
    public static String limitPath(String path) {
        return quotaZookeeper + path + "/" + limitNode;
    }

    /**
     * return the stat quota path associated with this
     * prefix.
     *
     * @param path the actual path in zookeeper
     * @return the stat quota path
     */
    public static String statPath(String path) {
        return quotaZookeeper + path + "/" + statNode;
    }

    /**
     * return the real path associated with this
     * quotaPath.
     *
     * @param quotaPath the quotaPath which's started with /zookeeper/quota
     * @return the real path associated with this quotaPath.
     */
    public static String trimQuotaPath(String quotaPath) {
        return quotaPath.substring(quotaZookeeper.length());
    }
}
