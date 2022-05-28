package org.apache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooKeeperTestable implements Testable {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperTestable.class);

    private final ClientCnxn clientCnxn;

    ZooKeeperTestable(ClientCnxn clientCnxn) {
        this.clientCnxn = clientCnxn;
    }

    @Override
    public void injectSessionExpiration() {
        LOG.info("injectSessionExpiration() called");

        clientCnxn.eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
        clientCnxn.eventThread.queueEventOfDeath();
        clientCnxn.state = ZooKeeper.States.CLOSED;
        clientCnxn.sendThread.getClientCnxnSocket().onClosing();
    }

    @Override
    public void queueEvent(WatchedEvent event) {
        LOG.info("queueEvent() called: {}", event);
        clientCnxn.eventThread.queueEvent(event);
    }

}
