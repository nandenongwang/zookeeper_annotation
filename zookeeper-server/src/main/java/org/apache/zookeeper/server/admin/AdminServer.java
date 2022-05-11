package org.apache.zookeeper.server.admin;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * 通过Http暴露的命令执行接口 【8080】
 * Interface for an embedded admin server that runs Commands. There is only one
 * functional implementation, JettyAdminServer. DummyAdminServer, which does
 * nothing, is used when we do not wish to run a server.
 */
@InterfaceAudience.Public
public interface AdminServer {

    void start() throws AdminServerException;

    void shutdown() throws AdminServerException;

    void setZooKeeperServer(ZooKeeperServer zkServer);

    @InterfaceAudience.Public
    class AdminServerException extends Exception {

        private static final long serialVersionUID = 1L;

        public AdminServerException(String message, Throwable cause) {
            super(message, cause);
        }

        public AdminServerException(Throwable cause) {
            super(cause);
        }

    }

}
