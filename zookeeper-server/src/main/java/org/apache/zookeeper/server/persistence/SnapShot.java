package org.apache.zookeeper.server.persistence;

import org.apache.zookeeper.server.DataTree;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 快照接口
 * snapshot interface for the persistence layer.
 * implement this interface for implementing
 * snapshots.
 */
public interface SnapShot {

    /**
     * 反序列化
     * deserialize a data tree from the last valid snapshot and
     * return the last zxid that was deserialized
     *
     * @param dt       the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException;

    /**
     * 序列化
     * persist the datatree and the sessions into a persistence storage
     *
     * @param dt       the datatree to be serialized
     * @param sessions the session timeouts to be serialized
     * @param name     the object name to store snapshot into
     * @param fsync    sync the snapshot immediately after write
     * @throws IOException
     */
    void serialize(DataTree dt, Map<Long, Integer> sessions, File name, boolean fsync) throws IOException;

    /**
     * 获取最近的快照文件
     * find the most recent snapshot file
     *
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;

    /**
     * 获取最近的快照信息
     * get information of the last saved/restored snapshot
     *
     * @return info of last snapshot
     */
    SnapshotInfo getLastSnapshotInfo();

    /**
     * free resources from this snapshot immediately
     *
     * @throws IOException
     */
    void close() throws IOException;

}
