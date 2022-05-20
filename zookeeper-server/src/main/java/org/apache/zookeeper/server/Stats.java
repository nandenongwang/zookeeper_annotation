package org.apache.zookeeper.server;

import java.util.Date;

/**
 * 服务端侧连接统计状态接口
 * Statistics on the ServerCnxn
 */
interface Stats {

    /**
     * 连接创建时间
     * Date/time the connection was established
     *
     * @since 3.3.0
     */
    Date getEstablished();

    /**
     * The number of requests that have been submitted but not yet
     * responded to.
     */
    long getOutstandingRequests();

    /**
     * 接收到总包数
     * Number of packets received
     */
    long getPacketsReceived();

    /**
     * 发送总包数
     * Number of packets sent (incl notifications)
     */
    long getPacketsSent();

    /**
     * 最小延时
     * Min latency in ms
     *
     * @since 3.3.0
     */
    long getMinLatency();

    /**
     * 平均延时
     * Average latency in ms
     *
     * @since 3.3.0
     */
    long getAvgLatency();

    /**
     * 最大延时
     * Max latency in ms
     *
     * @since 3.3.0
     */
    long getMaxLatency();

    /**
     * 最后操作名
     * Last operation performed by this connection
     *
     * @since 3.3.0
     */
    String getLastOperation();

    /**
     * 最后cxid
     * Last cxid of this connection
     *
     * @since 3.3.0
     */
    long getLastCxid();

    /**
     * 最后zxid
     * Last zxid of this connection
     *
     * @since 3.3.0
     */
    long getLastZxid();

    /**
     * 最后响应时间
     * Last time server sent a response to client on this connection
     *
     * @since 3.3.0
     */
    long getLastResponseTime();

    /**
     * 最后响应延时
     * Latency of last response to client on this connection in ms
     *
     * @since 3.3.0
     */
    long getLastLatency();

    /**
     * 重置状态统计
     * Reset counters
     *
     * @since 3.3.0
     */
    void resetStats();

}
