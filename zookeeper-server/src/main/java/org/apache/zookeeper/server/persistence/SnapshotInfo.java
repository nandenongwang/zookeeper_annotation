package org.apache.zookeeper.server.persistence;

/**
 * 快照信息
 * stores the zxid (as in its file name) and the last modified timestamp
 * of a snapshot file
 */
public class SnapshotInfo {

    /**
     * 最后已应用事务ID
     */
    public long zxid;

    /**
     *最后修改时间戳
     */
    public long timestamp;

    SnapshotInfo(long zxid, long timestamp) {
        this.zxid = zxid;
        this.timestamp = timestamp;
    }

}
