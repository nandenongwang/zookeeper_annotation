package org.apache.zookeeper.server.quorum;


/**
 * 选举算法
 */
public interface Election {

    Vote lookForLeader() throws InterruptedException;

    void shutdown();

}
