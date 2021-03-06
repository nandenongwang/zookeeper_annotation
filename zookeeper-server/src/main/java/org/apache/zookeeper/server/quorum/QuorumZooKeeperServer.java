package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * 所有集群角色server基类
 * Abstract base class for all ZooKeeperServers that participate in
 * a quorum.
 */
public abstract class QuorumZooKeeperServer extends ZooKeeperServer {

    public final QuorumPeer self;
    protected UpgradeableSessionTracker upgradeableSessionTracker;

    protected QuorumZooKeeperServer(FileTxnSnapLog logFactory, int tickTime, int minSessionTimeout,
                                    int maxSessionTimeout, int listenBacklog, ZKDatabase zkDb, QuorumPeer self) {
        super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, listenBacklog, zkDb, self.getInitialConfig(),
                self.isReconfigEnabled());
        this.self = self;
    }

    @Override
    protected void startSessionTracker() {
        upgradeableSessionTracker = (UpgradeableSessionTracker) sessionTracker;
        upgradeableSessionTracker.start();
    }

    /**
     *
     */
    public Request checkUpgradeSession(Request request) throws IOException, KeeperException {
        if (request.isThrottled()) {
            return null;
        }

        // If this is a request for a local session and it is to
        // create an ephemeral node, then upgrade the session and return
        // a new session request for the leader.

        // This is called by the request processor thread (either follower
        // or observer request processor), which is unique to a learner.
        // So will not be called concurrently by two threads.

        //本地session不能创建临时节点

        //region 已经是全局session或者非创建节点请求(必然也非创建临时节点)则无需升级session
        if ((request.type != OpCode.create && request.type != OpCode.create2 && request.type != OpCode.multi)
                || !upgradeableSessionTracker.isLocalSession(request.sessionId)) {
            return null;
        }
        //endregion

        //region 本地session创建节点请求、但非创建临时节点也无需升级session
        if (OpCode.multi == request.type) {
            MultiOperationRecord multiTransactionRecord = new MultiOperationRecord();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
            request.request.rewind();
            boolean containsEphemeralCreate = false;
            for (Op op : multiTransactionRecord) {
                if (op.getType() == OpCode.create || op.getType() == OpCode.create2) {
                    CreateRequest createRequest = (CreateRequest) op.toRequestRecord();
                    CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                    if (createMode.isEphemeral()) {
                        containsEphemeralCreate = true;
                        break;
                    }
                }
            }
            if (!containsEphemeralCreate) {
                return null;
            }
        } else {
            CreateRequest createRequest = new CreateRequest();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);
            request.request.rewind();
            CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
            if (!createMode.isEphemeral()) {
                return null;
            }
        }
        //endregion

        //region 本地session升级关闭、异常【默认开启】
        // Uh oh.  We need to upgrade before we can proceed.
        if (!self.isLocalSessionsUpgradingEnabled()) {
            throw new KeeperException.EphemeralOnLocalSessionException();
        }
        //endregion

        return makeUpgradeRequest(request.sessionId);
    }

    /**
     * 创建创建session请求、并在session管理器中标记该session升级中
     */
    private Request makeUpgradeRequest(long sessionId) {
        // Make sure to atomically check local session status, upgrade
        // session, and make the session creation request.  This is to
        // avoid another thread upgrading the session in parallel.
        synchronized (upgradeableSessionTracker) {
            if (upgradeableSessionTracker.isLocalSession(sessionId)) {
                int timeout = upgradeableSessionTracker.upgradeSession(sessionId);
                ByteBuffer to = ByteBuffer.allocate(4);
                to.putInt(timeout);
                return new Request(null, sessionId, 0, OpCode.createSession, to, null);
            }
        }
        return null;
    }

    /**
     * Implements the SessionUpgrader interface,
     *
     * @param sessionId
     */
    public void upgrade(long sessionId) {
        Request request = makeUpgradeRequest(sessionId);
        if (request != null) {
            LOG.info("Upgrading session 0x{}", Long.toHexString(sessionId));
            // This must be a global request
            submitRequest(request);
        }
    }

    @Override
    protected void setLocalSessionFlag(Request si) {
        // We need to set isLocalSession to tree for these type of request
        // so that the request processor can process them correctly.
        switch (si.type) {
            case OpCode.createSession:
                if (self.areLocalSessionsEnabled()) {
                    // All new sessions local by default.
                    si.setLocalSession(true);
                }
                break;
            case OpCode.closeSession:
                String reqType = "global";
                if (upgradeableSessionTracker.isLocalSession(si.sessionId)) {
                    si.setLocalSession(true);
                    reqType = "local";
                }
                LOG.info("Submitting {} closeSession request for session 0x{}", reqType, Long.toHexString(si.sessionId));
                break;
            default:
                break;
        }
    }

    @Override
    public void dumpConf(PrintWriter pwriter) {
        super.dumpConf(pwriter);

        pwriter.print("initLimit=");
        pwriter.println(self.getInitLimit());
        pwriter.print("syncLimit=");
        pwriter.println(self.getSyncLimit());
        pwriter.print("electionAlg=");
        pwriter.println(self.getElectionType());
        pwriter.print("electionPort=");
        pwriter.println(self.getElectionAddress().getAllPorts()
                .stream().map(Objects::toString).collect(Collectors.joining("|")));
        pwriter.print("quorumPort=");
        pwriter.println(self.getQuorumAddress().getAllPorts()
                .stream().map(Objects::toString).collect(Collectors.joining("|")));
        pwriter.print("peerType=");
        pwriter.println(self.getLearnerType().ordinal());
        pwriter.println("membership: ");
        pwriter.print(self.getQuorumVerifier().toString());
    }

    @Override
    protected void setState(State state) {
        this.state = state;
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("quorum_size", () -> {
            return self.getQuorumSize();
        });
    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.unregisterGauge("quorum_size");
    }

    @Override
    public void dumpMonitorValues(BiConsumer<String, Object> response) {
        super.dumpMonitorValues(response);
        response.accept("peer_state", self.getDetailedPeerState());
    }

}
