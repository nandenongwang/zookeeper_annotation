package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * 关闭管理服务 空实现
 * An AdminServer that does nothing.
 *
 * We use this class when we wish to disable the AdminServer. (This way we only
 * have to consider whether the server is enabled when we create the
 * AdminServer, which is handled by AdminServerFactory.)
 */
public class DummyAdminServer implements AdminServer {

    @Override
    public void start() throws AdminServerException {
    }

    @Override
    public void shutdown() throws AdminServerException {
    }

    @Override
    public void setZooKeeperServer(ZooKeeperServer zkServer) {
    }

}
