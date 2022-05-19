package org.apache.zookeeper.server.watch;

import org.apache.zookeeper.ZooDefs;

/**
 * 监视器模式
 */
public enum WatcherMode {

    /**
     * 标准模式 【非永久、非递归】
     */
    STANDARD(false, false),

    /**
     * 永久模式 【永久、非递归】
     */
    PERSISTENT(true, false),

    /**
     * 永久递归模式 【永久、向前递归】
     */
    PERSISTENT_RECURSIVE(true, true);

    public static final WatcherMode DEFAULT_WATCHER_MODE = WatcherMode.STANDARD;

    public static WatcherMode fromZooDef(int mode) {
        switch (mode) {
            case ZooDefs.AddWatchModes.persistent:
                return PERSISTENT;
            case ZooDefs.AddWatchModes.persistentRecursive:
                return PERSISTENT_RECURSIVE;
        }
        throw new IllegalArgumentException("Unsupported mode: " + mode);
    }

    private final boolean isPersistent;
    private final boolean isRecursive;

    WatcherMode(boolean isPersistent, boolean isRecursive) {
        this.isPersistent = isPersistent;
        this.isRecursive = isRecursive;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public boolean isRecursive() {
        return isRecursive;
    }
}
