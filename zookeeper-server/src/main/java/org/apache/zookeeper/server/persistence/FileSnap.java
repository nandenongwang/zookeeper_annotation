package org.apache.zookeeper.server.persistence;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * 数据快照
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {

    File snapDir;
    SnapshotInfo lastSnapshotInfo = null;
    private volatile boolean close = false;
    private static final int VERSION = 2;
    private static final long dbId = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public static final int SNAP_MAGIC = ByteBuffer.wrap("ZKSN".getBytes()).getInt();

    public static final String SNAPSHOT_FILE_PREFIX = "snapshot";

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * 获取上次快照信息
     * get information of the last saved/restored snapshot
     *
     * @return info of last snapshot
     */
    public SnapshotInfo getLastSnapshotInfo() {
        return this.lastSnapshotInfo;
    }

    /**
     * 反序列化最近的快照
     * deserialize a data tree from the most recent snapshot
     *
     * @return the zxid of the snapshot
     */
    @Override
    public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {
        //region 获取最近100个快照文件
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        List<File> snapList = findNValidSnapshots(100);
        if (snapList.size() == 0) {
            return -1L;
        }
        //endregion

        //region 反序列化出最新可用快照 【通常是最近的快照】
        File snap = null;
        long snapZxid = -1;
        boolean foundValid = false;
        for (File file : snapList) {
            snap = file;
            LOG.info("Reading snapshot {}", snap);
            snapZxid = Util.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);
            try (CheckedInputStream snapIS = SnapStream.getInputStream(snap)) {
                InputArchive ia = BinaryInputArchive.getArchive(snapIS);
                deserialize(dt, sessions, ia);
                SnapStream.checkSealIntegrity(snapIS, ia);

                // Digest feature was added after the CRC to make it backward
                // compatible, the older code can still read snapshots which
                // includes digest.
                //
                // To check the intact, after adding digest we added another
                // CRC check.
                if (dt.deserializeZxidDigest(ia, snapZxid)) {
                    SnapStream.checkSealIntegrity(snapIS, ia);
                }

                foundValid = true;
                break;
            } catch (IOException e) {
                LOG.warn("problem reading snap file {}", snap, e);
            }
        }
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        //endregion

        //region 更新快照进度【最新日志ID、最新快照信息等】
        dt.lastProcessedZxid = snapZxid;
        lastSnapshotInfo = new SnapshotInfo(dt.lastProcessedZxid, snap.lastModified() / 1000);

        // compare the digest if this is not a fuzzy snapshot, we want to compare
        // and find inconsistent asap.
        if (dt.getDigestFromLoadedSnapshot() != null) {
            dt.compareSnapshotDigests(dt.lastProcessedZxid);
        }
        return dt.lastProcessedZxid;
        //endregion
    }

    /**
     * 反序列化 【依次读取文件头、sessionMap、dateTree】
     * deserialize the datatree from an inputarchive
     *
     * @param dt       the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia       the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions, InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers " + header.getMagic() + " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt, ia, sessions);
    }

    /**
     * 获取最近的快照文件
     * find the most recent snapshot in the database.
     *
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * 找到指定数目的最近的可用快照文件
     * find the last (maybe) valid n snapshots. this does some
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent
     * will be first on the list.
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     */
    protected List<File> findNValidSnapshots(int n) {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                if (SnapStream.isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.warn("invalid snapshot {}", f, e);
            }
        }
        return list;
    }

    /**
     * 找到指定数目的最近的快照文件
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<>();
        for (File f : files) {
            if (count == n) {
                break;
            }
            if (Util.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * 序列化快照文件 【文件头、sessionMap、dataTree】
     * serialize the datatree and sessions
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa       the output archive to serialize into
     * @param header   the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt, Map<Long, Integer> sessions, OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if (header == null) {
            throw new IllegalStateException("Snapshot's not open for writing: uninitialized header");
        }
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt, oa, sessions);
    }

    /**
     * 序列化快照文件并更新快照进度
     * serialize the datatree and session into the file snapshot
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     * @param fsync    sync the file immediately after write
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot, boolean fsync) throws IOException {
        if (!close) {
            try (CheckedOutputStream snapOS = SnapStream.getOutputStream(snapShot, fsync)) {
                OutputArchive oa = BinaryOutputArchive.getArchive(snapOS);
                FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
                serialize(dt, sessions, oa, header);
                SnapStream.sealStream(snapOS, oa);

                // Digest feature was added after the CRC to make it backward
                // compatible, the older code cal still read snapshots which
                // includes digest.
                //
                // To check the intact, after adding digest we added another
                // CRC check.
                if (dt.serializeZxidDigest(oa)) {
                    SnapStream.sealStream(snapOS, oa);
                }

                lastSnapshotInfo = new SnapshotInfo(
                        Util.getZxidFromName(snapShot.getName(), SNAPSHOT_FILE_PREFIX),
                        snapShot.lastModified() / 1000);
            }
        } else {
            throw new IOException("FileSnap has already been closed");
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}
