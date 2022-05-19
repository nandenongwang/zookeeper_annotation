package org.apache.zookeeper.server.watch;

import org.apache.zookeeper.Watcher;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监听项监听模式管理器
 */
class WatcherModeManager {
    private final Map<Key/* watcher-path */, WatcherMode> watcherModes = new ConcurrentHashMap<>();
    private final AtomicInteger recursiveQty = new AtomicInteger(0);

    private static class Key {
        private final Watcher watcher;
        private final String path;

        Key(Watcher watcher, String path) {
            this.watcher = watcher;
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return watcher.equals(key.watcher) && path.equals(key.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(watcher, path);
        }
    }

    // VisibleForTesting
    Map<Key, WatcherMode> getWatcherModes() {
        return watcherModes;
    }

    void setWatcherMode(Watcher watcher, String path, WatcherMode mode) {
        if (mode == WatcherMode.DEFAULT_WATCHER_MODE) {
            removeWatcher(watcher, path);
        } else {
            adjustRecursiveQty(watcherModes.put(new Key(watcher, path), mode), mode);
        }
    }

    WatcherMode getWatcherMode(Watcher watcher, String path) {
        return watcherModes.getOrDefault(new Key(watcher, path), WatcherMode.DEFAULT_WATCHER_MODE);
    }

    void removeWatcher(Watcher watcher, String path) {
        adjustRecursiveQty(watcherModes.remove(new Key(watcher, path)), WatcherMode.DEFAULT_WATCHER_MODE);
    }

    int getRecursiveQty() {
        return recursiveQty.get();
    }

    // recursiveQty is an optimization to avoid having to walk the map every time this value is needed
    private void adjustRecursiveQty(WatcherMode oldMode, WatcherMode newMode) {
        if (oldMode == null) {
            oldMode = WatcherMode.DEFAULT_WATCHER_MODE;
        }
        if (oldMode.isRecursive() != newMode.isRecursive()) {
            if (newMode.isRecursive()) {
                recursiveQty.incrementAndGet();
            } else {
                recursiveQty.decrementAndGet();
            }
        }
    }
}
