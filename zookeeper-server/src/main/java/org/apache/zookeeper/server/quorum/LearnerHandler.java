package org.apache.zookeeper.server.quorum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * 议长处理其他议员请求处理线程
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    public static final String LEADER_CLOSE_SOCKET_ASYNC = "zookeeper.leader.closeSocketAsync";

    public static final boolean closeSocketAsync = Boolean
            .parseBoolean(ConfigUtils.getPropertyBackwardCompatibleWay(LEADER_CLOSE_SOCKET_ASYNC));

    static {
        LOG.info("{} = {}", LEADER_CLOSE_SOCKET_ASYNC, closeSocketAsync);
    }

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    AtomicBoolean sockBeingClosed = new AtomicBoolean(false);

    final LearnerMaster learnerMaster;

    /**
     * Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader.
     */
    volatile long tickOfNextAckDeadline;

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    String getRemoteAddress() {
        return sock == null ? "<null>" : sock.getRemoteSocketAddress().toString();
    }

    protected int version = 0x1;

    int getVersion() {
        return version;
    }

    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();
    private final AtomicLong queuedPacketsSize = new AtomicLong();

    protected final AtomicLong packetsReceived = new AtomicLong();
    protected final AtomicLong packetsSent = new AtomicLong();

    protected final AtomicLong requestsReceived = new AtomicLong();

    protected volatile long lastZxid = -1;

    public synchronized long getLastZxid() {
        return lastZxid;
    }

    protected final Date established = new Date();

    public Date getEstablished() {
        return (Date) established.clone();
    }

    /**
     * Marker packets would be added to quorum packet queue after every
     * markerPacketInterval packets.
     * It is ok if packetCounter overflows.
     */
    private final int markerPacketInterval = 1000;
    private AtomicInteger packetCounter = new AtomicInteger();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {

        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            if (currentZxid == zxid) {
                currentTime = nextTime;
                currentZxid = nextZxid;
                nextTime = 0;
                nextZxid = 0;
            } else if (nextZxid == zxid) {
                LOG.warn(
                        "ACK for 0x{} received before ACK for 0x{}",
                        Long.toHexString(zxid),
                        Long.toHexString(currentZxid));
                nextTime = 0;
                nextZxid = 0;
            }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < learnerMaster.syncTimeout());
            }
        }

    }

    private final SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private static class MarkerQuorumPacket extends QuorumPacket {

        long time;

        MarkerQuorumPacket(long time) {
            this.time = time;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MarkerQuorumPacket that = (MarkerQuorumPacket) o;
            return time == that.time;
        }

    }

    //region 输入输出
    /**
     * 连接输入接口
     */
    private BinaryInputArchive ia;

    /**
     * 连接输出接口
     */
    private BinaryOutputArchive oa;

    /**
     * 输入流
     */
    private final BufferedInputStream bufferedInput;

    /**
     * 输出流
     */
    private BufferedOutputStream bufferedOutput;

    // for test only
    protected void setOutputArchive(BinaryOutputArchive oa) {
        this.oa = oa;
    }

    protected void setBufferedOutput(BufferedOutputStream bufferedOutput) {
        this.bufferedOutput = bufferedOutput;
    }

    //endregion

    protected final MessageTracker messageTracker;

    /**
     * 消息发送线程是否已启动
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /**
     * For testing purpose, force learnerMaster to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private boolean forceSnapSync = false;

    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    private boolean needOpPacket = true;

    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;

    /**
     * 数据同步流控器
     * for sync throttling
     */
    private LearnerSyncThrottler syncThrottler = null;

    LearnerHandler(Socket sock, BufferedInputStream bufferedInput, LearnerMaster learnerMaster) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.learnerMaster = learnerMaster;
        this.bufferedInput = bufferedInput;

        if (Boolean.getBoolean(FORCE_SNAP_SYNC)) {
            forceSnapSync = true;
            LOG.info("Forcing snapshot sync is enabled");
        }

        try {
            QuorumAuthServer authServer = learnerMaster.getQuorumAuthServer();
            if (authServer != null) {
                authServer.authenticate(sock, new DataInputStream(bufferedInput));
            }
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection", sock.getRemoteSocketAddress(), e);
            closeSocket();

            throw new SaslException("Authentication failure: " + e.getMessage());
        }

        this.messageTracker = new MessageTracker(MessageTracker.BUFFERED_MESSAGE_SIZE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * 将待发送队列中的消息序列化并写出
     * This method will use the thread to send packets added to the
     * queuedPackets list
     */
    private void sendPackets() throws InterruptedException {
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                ServerMetrics.getMetrics().LEARNER_HANDLER_QP_SIZE.add(Long.toString(this.sid), queuedPackets.size());

                if (p instanceof MarkerQuorumPacket) {
                    MarkerQuorumPacket m = (MarkerQuorumPacket) p;
                    ServerMetrics.getMetrics().LEARNER_HANDLER_QP_TIME
                            .add(Long.toString(this.sid), (System.nanoTime() - m.time) / 1000000L);
                    continue;
                }

                queuedPacketsSize.addAndGet(-packetSize(p));
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }

                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                    if (p.getType() == Leader.PING) {
                        traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                    }
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }

                // Log the zxid of the last request, if it is a valid zxid.
                if (p.getZxid() > 0) {
                    lastZxid = p.getZxid();
                }
                oa.writeRecord(p, "packet");
                packetsSent.incrementAndGet();
                messageTracker.trackSent(p.getType());
            } catch (IOException e) {
                LOG.error("Exception while sending packets in LearnerHandler", e);
                // this will cause everything to shutdown on
                // this learner handler and will help notify
                // the learner/observer instantaneously
                closeSocket();
                break;
            }
        }
    }

    /**
     * 获取类型名
     */
    public static String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
                DataInputStream dis = new DataInputStream(bis);
                try {
                    long id = dis.readLong();
                    mess = " sessionid = " + id;
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }

                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            case Leader.DIFF:
                type = "DIFF";
                break;
            case Leader.TRUNC:
                type = "TRUNC";
                break;
            case Leader.SNAP:
                type = "SNAP";
                break;
            case Leader.ACKEPOCH:
                type = "ACKEPOCH";
                break;
            case Leader.SYNC:
                type = "SYNC";
                break;
            case Leader.INFORM:
                type = "INFORM";
                break;
            case Leader.COMMITANDACTIVATE:
                type = "COMMITANDACTIVATE";
                break;
            case Leader.INFORMANDACTIVATE:
                type = "INFORMANDACTIVATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            learnerMaster.addLearnerHandler(this);
            tickOfNextAckDeadline = learnerMaster.getTickOfInitialAckDeadline();

            //region 获取learner输入、输出器
            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);
            //endregion

            //region 读取并解析注册包【FOLLOWERINFO、OBSERVERINFO、等待多数follower连接并晋升任期】
            QuorumPacket qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO) {
                LOG.error("First packet {} is not FOLLOWERINFO or OBSERVERINFO!", qp.toString());

                return;
            }

            if (learnerMaster instanceof ObserverMaster && qp.getType() != Leader.OBSERVERINFO) {
                throw new IOException("Non observer attempting to connect to ObserverMaster. type = " + qp.getType());
            }
            byte[] learnerInfoData = qp.getData();
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                if (learnerInfoData.length >= 8) {
                    this.sid = bbsid.getLong();
                }
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > learnerMaster.getQuorumVerifierVersion()) {
                        throw new IOException("Follower is ahead of the leader (has a later activated configuration)");
                    }
                }
            } else {
                this.sid = learnerMaster.getAndDecrementFollowerCounter();
            }

            String followerInfo = learnerMaster.getPeerInfo(this.sid);
            if (followerInfo.isEmpty()) {
                LOG.info(
                        "Follower sid: {} not in the current config {}",
                        this.sid,
                        Long.toHexString(learnerMaster.getQuorumVerifierVersion()));
            } else {
                LOG.info("Follower sid: {} : info : {}", this.sid, followerInfo);
            }

            if (qp.getType() == Leader.OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }

            learnerMaster.registerLearnerHandlerBean(this, sock);

            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            long peerLastZxid;
            StateSummary ss = null;
            long zxid = qp.getZxid();
            long newEpoch = learnerMaster.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);
            //endregion

            //region 发送leader信息包并等待任期确认包 【等待多数follower确认】
            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            } else {
                byte[] ver = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                messageTracker.trackSent(Leader.LEADERINFO);
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                messageTracker.trackReceived(ackEpochPacket.getType());
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error("{} is not ACKEPOCH", ackEpochPacket.toString());
                    return;
                }
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            }
            //endregion

            //region 获取learner最后应用提案ID
            peerLastZxid = ss.getLastZxid();
            //endregion

            //region 根据learner最后应用提案ID判断是否需要同步快照 【并将待同步数据写入待发送缓存】
            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases
            boolean needSnap = syncFollower(peerLastZxid, learnerMaster);
            //endregion

            //region 是否需要对同步线程数限流 【follower不需要、observer需要】
            // syncs between followers and the leader are exempt from throttling because it
            // is important to keep the state of quorum servers up-to-date. The exempted syncs
            // are counted as concurrent syncs though
            boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
            //endregion

            //region 开启同步流控器并发送快照包(若需要传输快照)
            /* if we are not truncating or sending a diff just send a snapshot */
            if (needSnap) {
                syncThrottler = learnerMaster.getLearnerSnapSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
                try {
                    long zxidToSend = learnerMaster.getZKDatabase().getDataTreeLastProcessedZxid();
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    messageTracker.trackSent(Leader.SNAP);
                    bufferedOutput.flush();

                    LOG.info(
                            "Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                                    + "send zxid of db as 0x{}, {} concurrent snapshot sync, "
                                    + "snapshot sync was {} from throttle",
                            Long.toHexString(peerLastZxid),
                            Long.toHexString(leaderLastZxid),
                            Long.toHexString(zxidToSend),
                            syncThrottler.getSyncInProgress(),
                            exemptFromThrottle ? "exempt" : "not exempt");
                    // Dump data to peer
                    learnerMaster.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    ServerMetrics.getMetrics().SNAP_COUNT.add(1);
                }
            } else {
                syncThrottler = learnerMaster.getLearnerDiffSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
                ServerMetrics.getMetrics().DIFF_COUNT.add(1);
            }
            //endregion

            //region 发送任期晋升包
            LOG.debug("Sending NEWLEADER message to {}", sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, learnerMaster.getQuorumVerifierBytes(), null);
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();
            //endregion

            //region 开启发送线程
            // Start thread that blast packets in the queue to learner
            startSendingPackets();
            //endregion

            //region 等待多数learner任期晋升响应
            /*
             * Have to wait for the first ACK, wait until
             * the learnerMaster is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK, but received packet: {}", packetToString(qp));
                return;
            }

            LOG.debug("Received NEWLEADER-ACK message from {}", sid);

            learnerMaster.waitForNewLeaderAck(getSid(), qp.getZxid());
            //endregion

            //region 等待leader数据同步完毕后启动完成
            syncLimitCheck.start();
            // sync ends when NEWLEADER-ACK is received
            syncThrottler.endSync();
            if (needSnap) {
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
            } else {
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
            }
            syncThrottler = null;

            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(learnerMaster.syncTimeout());
            /*
             * Wait until learnerMaster starts up
             */
            learnerMaster.waitForStartup();
            //endregion

            //region 发送同步完毕包、指示learner开始正常工作
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            LOG.debug("Sending UPTODATE message to {}", sid);
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));
            //endregion

            //region 循环接受处理learner工作状态时消息
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");
                messageTracker.trackReceived(qp.getType());

                if (LOG.isTraceEnabled()) {
                    long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                    if (qp.getType() == Leader.PING) {
                        traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                    }
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = learnerMaster.getTickOfNextAckDeadline();

                packetsReceived.incrementAndGet();

                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                    //region follower向leader确认提案zxid已存储
                    case Leader.ACK:
                        if (this.learnerType == LearnerType.OBSERVER) {
                            LOG.debug("Received ACK from Observer {}", this.sid);
                        }
                        syncLimitCheck.updateAck(qp.getZxid());
                        learnerMaster.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                        break;
                    //endregion

                    //region learner回复leader Ping请求
                    case Leader.PING:
                        // Process the touches
                        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
                        DataInputStream dis = new DataInputStream(bis);
                        while (dis.available() > 0) {
                            long sess = dis.readLong();
                            int to = dis.readInt();
                            learnerMaster.touch(sess, to);
                        }
                        break;
                    //endregion

                    //region 校验客户端sessionId
                    case Leader.REVALIDATE:
                        ServerMetrics.getMetrics().REVALIDATE_COUNT.add(1);
                        learnerMaster.revalidateSession(qp, this);
                        break;
                    //endregion

                    //region follower向leader转发的事务请求、提交到leader处理链进行处理
                    case Leader.REQUEST: {
                        bb = ByteBuffer.wrap(qp.getData());
                        sessionId = bb.getLong();
                        cxid = bb.getInt();
                        type = bb.getInt();
                        bb = bb.slice();
                        Request si;
                        if (type == OpCode.sync) {
                            si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                        } else {
                            si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                        }
                        si.setOwner(this);
                        learnerMaster.submitLearnerRequest(si);
                        requestsReceived.incrementAndGet();
                        break;
                    }
                    //endregion

                    //region 其他
                    default:
                        LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                        break;
                    //endregion
                }
            }
            //endregion
        }
        //region 异常处理
        catch (IOException e) {
            LOG.error("Unexpected exception in LearnerHandler: ", e);
            closeSocket();
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
        } catch (SyncThrottleException e) {
            LOG.error("too many concurrent sync.", e);
            syncThrottler = null;
        } catch (Exception e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
            throw e;
        } finally {
            if (syncThrottler != null) {
                syncThrottler.endSync();
                syncThrottler = null;
            }
            String remoteAddr = getRemoteAddress();
            LOG.warn("******* GOODBYE {} ********", remoteAddr);
            messageTracker.dumpToLog(remoteAddr);
            shutdown();
        }
        //endregion
    }

    /**
     * 启动消息发送线程
     * Start thread that will forward any packet in the queue to the follower
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Tests need not send marker packets as they are only needed to
     * log quorum packet delays
     */
    protected boolean shouldSendMarkerPacketForLogging() {
        return true;
    }

    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     *
     * @param peerLastZxid
     * @param learnerMaster
     * @return true if snapshot transfer is needed.
     */
    boolean/* 是否需要传输快照 */ syncFollower(long peerLastZxid/* learner最后日志ID */, LearnerMaster learnerMaster/* leader */) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the learnerMaster. If the same learner come
         * back to sync with learnerMaster using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         */
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued
        long currentZxid = peerLastZxid;
        boolean needSnap = true;
        ZKDatabase db = learnerMaster.getZKDatabase();
        boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
        ReentrantReadWriteLock lock = db.getLogLock();
        ReadLock rl = lock.readLock();
        try {
            rl.lock();
            //region 已提交提案缓存中最大、最小提案ID 【如未缓存取最后应用提案ID】
            long maxCommittedLog = db.getmaxCommittedLog();
            long minCommittedLog = db.getminCommittedLog();
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Learner sid: {} maxCommittedLog=0x{}"
                            + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                            + " peerLastZxid=0x{}",
                    getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid));

            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that committedLog is empty. In that case
                 * setting these value to the latest txn in learnerMaster db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }
            //endregion

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and learnerMaster is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUNC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             */
            //region 1、强制使用快照 【默认false】
            if (forceSnapSync) {
                // Force learnerMaster to use snapshot to sync with follower
                LOG.warn("Forcing snapshot sync - should not see this in production");
            }
            //endregion

            //region 2、learner最后应用ID与leader最后应用ID相等 【已经是同步状态、发送空DIFF包】
            else if (lastProcessedZxid == peerLastZxid) {
                // Follower is already sync with us, send empty diff
                LOG.info(
                        "Sending DIFF zxid=0x{} for peer sid: {}",
                        Long.toHexString(peerLastZxid),
                        getSid());
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            }
            //endregion

            //region 3、learner最后应用ID大于leader最后应用ID且非晋升后任期ID 【存在脏数据、发送TRUNC从最大缓存处丢弃和开始转发】
            else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                // Newer than committedLog, send trunc and done
                LOG.debug(
                        "Sending TRUNC to follower zxidToSend=0x{} for peer sid:{}",
                        Long.toHexString(maxCommittedLog),
                        getSid());
                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            }
            //endregion

            //region 4、learner最后应用ID在缓存提案间 【设置无需快照、直接将之后缓存的提案DIFF发送】
            else if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                // Follower is within commitLog range
                LOG.info("Using committedLog for peer sid: {}", getSid());
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                currentZxid = queueCommittedProposals(itr, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            }
            //endregion

            //region 5、learner最后应用ID小于缓存最小ID
            else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                // Use txnlog and committedLog to sync

                //region 计算最大允许发送日志大小
                // Calculate sizeLimit that we allow to retrieve txnlog from disk
                long sizeLimit = db.calculateTxnLogSizeLimit();
                //endregion

                //region 读取限制数目的提案日志尝试发送
                // This method can return empty iterator if the requested zxid
                // is older than on-disk txnlog
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: {}", getSid());
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);

                    //region 差距大于快照阈值 【清空发送缓存、默认使用快照同步】
                    if (currentZxid < minCommittedLog) {
                        LOG.info(
                                "Detected gap between end of txnlog: 0x{} and start of committedLog: 0x{}",
                                Long.toHexString(currentZxid),
                                Long.toHexString(minCommittedLog));
                        currentZxid = peerLastZxid;
                        // Clear out currently queued requests and revert
                        // to sending a snapshot.
                        queuedPackets.clear();
                        needOpPacket = true;
                    }
                    //endregion

                    //region 差距小于快照阈值 【不需要快照、从日志迭代器中读取发送全部差距日志】
                    else {
                        LOG.debug("Queueing committedLog 0x{}", Long.toHexString(currentZxid));
                        Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                        currentZxid = queueCommittedProposals(committedLogItr, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                    //endregion
                }
                //endregion

                //region 关闭日志文件迭代器
                // closing the resources
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
                //endregion
            }
            //endregion

            //region 其他
            else {
                LOG.warn(
                        "Unhandled scenario for peer sid: {} maxCommittedLog=0x{}"
                                + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                                + " peerLastZxid=0x{} txnLogSyncEnabled={}",
                        getSid(),
                        Long.toHexString(maxCommittedLog),
                        Long.toHexString(minCommittedLog),
                        Long.toHexString(lastProcessedZxid),
                        Long.toHexString(peerLastZxid),
                        txnLogSyncEnabled);
            }
            //endregion

            //region 若需要快照【将拍摄快照传输】、使用最后应用ID作为转发开始ID
            if (needSnap) {
                currentZxid = db.getDataTreeLastProcessedZxid();
            }
            //endregion

            //region 开始转发
            LOG.debug("Start forwarding 0x{} for peer sid: {}", Long.toHexString(currentZxid), getSid());
            leaderLastZxid = learnerMaster.startForwarding(this, currentZxid);
            //endregion
        } finally {
            rl.unlock();
        }

        //region 默认发送快照
        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: {} fall back to use snapshot", getSid());
            needSnap = true;
        }
        //endregion
        return needSnap;
    }

    /**
     * 将要同步的已提交提案存入待发送缓冲队列中 【learner最后应用ID,最大缓存ID】
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     *
     * @param itr               iterator point to the proposals
     * @param peerLastZxid      last zxid seen by the follower
     * @param maxZxid           max zxid of the proposal to queue, null if no limit
     * @param lastCommittedZxid when sending diff, we need to send lastCommittedZxid
     *                          on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    protected long queueCommittedProposals(Iterator<Proposal> itr/* 可读取日志【已提交提案缓存|日志迭代器】 */,
                                           long peerLastZxid/* follower最后应用ID、从此处开始传输 */,
                                           Long maxZxid/* 传输截止ID、null表示不进行限制 */,
                                           Long lastCommittedZxid/* 最大缓存提案ID*/) {
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous
        // proposal Id.
        long prevProposalZxid = -1;
        while (itr.hasNext()) {
            Proposal propose = itr.next();
            long packetZxid = propose.packet.getZxid();

            //region 达到最大限制ID、跳出
            // abort if we hit the limit
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }
            //endregion

            //region 跳过比开始ID小的提案
            // skip the proposals the peer already has
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }
            //endregion

            //region 判断首次操作包是DIFF或TRUNC
            // If we are sending the first packet, figure out whether to trunc
            // or diff
            if (needOpPacket) {
                //region learner最后应用ID在缓存中、发送DIFF包
                // Send diff when we see the follower's zxid in our history
                if (packetZxid == peerLastZxid) {
                    LOG.info(
                            "Sending DIFF zxid=0x{}  for peer sid: {}",
                            Long.toHexString(lastCommittedZxid),
                            getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                    continue;
                }
                //endregion

                //region todo
                if (isPeerNewEpochZxid) {
                    // Send diff and fall through if zxid is of a new-epoch
                    LOG.info(
                            "Sending DIFF zxid=0x{}  for peer sid: {}",
                            Long.toHexString(lastCommittedZxid),
                            getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                }
                //endregion

                //region learn存在脏数据、TRUNC
                else if (packetZxid > peerLastZxid) {
                    // Peer have some proposals that the learnerMaster hasn't seen yet
                    // it may used to be a leader
                    if (ZxidUtils.getEpochFromZxid(packetZxid) != ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() + " peer zxid is from different epoch");
                        return queuedZxid;
                    }

                    LOG.info(
                            "Sending TRUNC zxid=0x{}  for peer sid: {}",
                            Long.toHexString(prevProposalZxid),
                            getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
                //endregion
            }
            //endregion

            //region
            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue
                // or there is a duplicate txn in a given iterator
                continue;
            }
            //endregion


            //region 依次传输提案再提交提案
            // Since this is already a committed proposal, we need to follow
            // it by a commit packet
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            //endregion
            queuedZxid = packetZxid;
        }

        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info(
                    "Sending TRUNC zxid=0x{}  for peer sid: {}",
                    Long.toHexString(lastCommittedZxid),
                    getSid());
            queueOpPacket(Leader.DIFF, lastCommittedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.clear();
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }

        closeSocket();

        this.interrupt();
        learnerMaster.removeLearnerHandler(this);
        learnerMaster.unregisterLearnerHandlerBean(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * 给连接方发送ping消息
     * ping calls from the learnerMaster to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            id = learnerMaster.getLastProposed();
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     *
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }

    /**
     * 发送消息
     */
    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
        // Add a MarkerQuorumPacket at regular intervals.
        if (shouldSendMarkerPacketForLogging() && packetCounter.getAndIncrement() % markerPacketInterval == 0) {
            queuedPackets.add(new MarkerQuorumPacket(System.nanoTime()));
        }
        queuedPacketsSize.addAndGet(packetSize(p));
    }

    static long packetSize(QuorumPacket p) {
        /* Approximate base size of QuorumPacket: int + long + byte[] + List */
        long size = 4 + 8 + 8 + 8;
        byte[] data = p.getData();
        if (data != null) {
            size += data.length;
        }
        return size;
    }

    public boolean synced() {
        return isAlive() && learnerMaster.getCurrentTick() <= tickOfNextAckDeadline;
    }

    public synchronized Map<String, Object> getLearnerHandlerInfo() {
        Map<String, Object> info = new LinkedHashMap<>(9);
        info.put("remote_socket_address", getRemoteAddress());
        info.put("sid", getSid());
        info.put("established", getEstablished());
        info.put("queued_packets", queuedPackets.size());
        info.put("queued_packets_size", queuedPacketsSize.get());
        info.put("packets_received", packetsReceived.longValue());
        info.put("packets_sent", packetsSent.longValue());
        info.put("requests", requestsReceived.longValue());
        info.put("last_zxid", getLastZxid());

        return info;
    }

    public synchronized void resetObserverConnectionStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        requestsReceived.set(0);

        lastZxid = -1;
    }

    /**
     * For testing, return packet queue
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }

    void closeSocket() {
        if (sock != null && !sock.isClosed() && sockBeingClosed.compareAndSet(false, true)) {
            if (closeSocketAsync) {
                LOG.info("Asynchronously closing socket to learner {}.", getSid());
                closeSockAsync();
            } else {
                LOG.info("Synchronously closing socket to learner {}.", getSid());
                closeSockSync();
            }
        }
    }

    void closeSockAsync() {
        final Thread closingThread = new Thread(() -> closeSockSync(), "CloseSocketThread(sid:" + this.sid);
        closingThread.setDaemon(true);
        closingThread.start();
    }

    void closeSockSync() {
        try {
            if (sock != null) {
                long startTime = Time.currentElapsedTime();
                sock.close();
                ServerMetrics.getMetrics().SOCKET_CLOSING_TIME.add(Time.currentElapsedTime() - startTime);
            }
        } catch (IOException e) {
            LOG.warn("Ignoring error closing connection to learner {}", getSid(), e);
        }
    }
}
