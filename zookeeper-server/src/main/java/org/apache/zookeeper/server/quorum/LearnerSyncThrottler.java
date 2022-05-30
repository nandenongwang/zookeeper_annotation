package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 限制并发同步线程数量
 * 开始同步时调用beginSync()
 * 结束同步时调用endSync()
 * 一般不限制follower、beginSync时essential参数为true、不进行流控
 * Utility class to limit the number of concurrent syncs from a leader to
 * observers and followers or from a follower to observers.  {@link LearnerHandler}
 * objects should call {@link #beginSync(boolean)} before sending a sync and
 * {@link #endSync()} after finishing, successfully or not.
 */
public class LearnerSyncThrottler {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerSyncThrottler.class);

    private final Object countSyncObject = new Object();

    /**
     * 正在同步线程数
     */
    private int syncInProgress;

    /**
     * 最大同步线程数
     */
    private volatile int maxConcurrentSyncs;

    public enum SyncType {
        DIFF,
        SNAP
    }

    private final SyncType syncType;

    /**
     * Constructs a new instance limiting the concurrent number of syncs to
     * <code>maxConcurrentSyncs</code>.
     *
     * @param maxConcurrentSyncs maximum concurrent number of syncs
     * @param syncType           either a snapshot sync or a txn-based diff sync
     * @throws java.lang.IllegalArgumentException when <code>maxConcurrentSyncs</code>
     *                                            is less than 1
     */
    public LearnerSyncThrottler(int maxConcurrentSyncs, SyncType syncType) throws IllegalArgumentException {
        if (maxConcurrentSyncs <= 0) {
            String errorMsg = "maxConcurrentSyncs must be positive, was " + maxConcurrentSyncs;
            throw new IllegalArgumentException(errorMsg);
        }

        this.maxConcurrentSyncs = maxConcurrentSyncs;
        this.syncType = syncType;

        synchronized (countSyncObject) {
            syncInProgress = 0;
        }
    }

    /**
     * Indicates that a new sync is about to be sent.
     *
     * @param essential if <code>true</code>, do not throw an exception even
     *                  if throttling limit is reached
     * @throws SyncThrottleException if throttling limit has been exceeded
     *                               and <code>essential == false</code>,
     *                               even after waiting for the timeout
     *                               period, if any
     * @throws InterruptedException  if thread is interrupted while trying
     *                               to start a sync; cannot happen if
     *                               timeout is zero
     */
    protected void beginSync(boolean essential) throws SyncThrottleException, InterruptedException {

        synchronized (countSyncObject) {
            if (essential || syncInProgress < maxConcurrentSyncs) {
                syncInProgress++;
            } else {
                throw new SyncThrottleException(syncInProgress + 1, maxConcurrentSyncs, syncType);
            }
        }
    }

    /**
     * Indicates that a sync has been completed.
     */
    public void endSync() {
        int newCount;
        synchronized (countSyncObject) {
            syncInProgress--;
            newCount = syncInProgress;
            countSyncObject.notify();
        }

        if (newCount < 0) {
            String errorMsg = "endSync() called incorrectly; current sync count is " + newCount;
            LOG.error(errorMsg);
        }
    }

    public void setMaxConcurrentSyncs(int maxConcurrentSyncs) {
        this.maxConcurrentSyncs = maxConcurrentSyncs;
    }

    public int getSyncInProgress() {
        return syncInProgress;
    }

}
