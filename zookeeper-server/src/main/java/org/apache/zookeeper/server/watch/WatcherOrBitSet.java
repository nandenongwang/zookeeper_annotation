package org.apache.zookeeper.server.watch;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.util.BitHashSet;

import java.util.Set;

/**
 * 将要触发的watcher集合 【默认使用watchers、优化管理器使用watcherBits】
 */
public class WatcherOrBitSet {

    private Set<Watcher> watchers;
    private BitHashSet watcherBits;

    public WatcherOrBitSet(final Set<Watcher> watchers) {
        this.watchers = watchers;
    }

    public WatcherOrBitSet(final BitHashSet watcherBits) {
        this.watcherBits = watcherBits;
    }

    public boolean contains(Watcher watcher) {
        if (watchers == null) {
            return false;
        }
        return watchers.contains(watcher);
    }

    public boolean contains(int watcherBit) {
        if (watcherBits == null) {
            return false;
        }
        return watcherBits.contains(watcherBit);
    }

    public int size() {
        if (watchers != null) {
            return watchers.size();
        }
        if (watcherBits != null) {
            return watcherBits.size();
        }
        return 0;
    }

}
