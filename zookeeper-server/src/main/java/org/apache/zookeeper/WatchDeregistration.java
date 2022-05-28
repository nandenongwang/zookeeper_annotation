package org.apache.zookeeper;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;

import java.util.Map;
import java.util.Set;

/**
 * 监听器注销
 * Handles the special case of removing watches which has registered for a
 * client path
 */
public class WatchDeregistration {

    private final String clientPath;
    private final Watcher watcher;
    private final WatcherType watcherType;
    private final boolean local;
    private final ZKWatchManager zkManager;

    public WatchDeregistration(String clientPath, Watcher watcher, WatcherType watcherType, boolean local, ZKWatchManager zkManager) {
        this.clientPath = clientPath;
        this.watcher = watcher;
        this.watcherType = watcherType;
        this.local = local;
        this.zkManager = zkManager;
    }

    /**
     * Unregistering watcher that was added on path.
     *
     * @param rc the result code of the operation that attempted to remove
     *           watch on the path.
     */
    public Map<EventType, Set<Watcher>> unregister(int rc) throws KeeperException {
        return zkManager.removeWatcher(clientPath, watcher, watcherType, local, rc);
    }

    /**
     * Returns client path which has specified for unregistering its watcher
     *
     * @return client path
     */
    public String getClientPath() {
        return clientPath;
    }

}
