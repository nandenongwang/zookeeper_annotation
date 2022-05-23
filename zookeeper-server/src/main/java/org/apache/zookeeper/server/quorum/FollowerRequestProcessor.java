package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * follower处理器 【与observer处理器基本一致】
 * 1、检测升级session
 * 2、转发事务请求给leader处理
 * 3、commit等待leader请求完成通知
 * This RequestProcessor forwards any requests that modify the state of the
 * system to the Leader.
 */

public class FollowerRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessor.class);

    public static final String SKIP_LEARNER_REQUEST_TO_NEXT_PROCESSOR = "zookeeper.follower.skipLearnerRequestToNextProcessor";

    private final boolean skipLearnerRequestToNextProcessor;

    FollowerZooKeeperServer zks;

    RequestProcessor nextProcessor;

    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<>();

    boolean finished = false;

    public FollowerRequestProcessor(FollowerZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("FollowerRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        this.skipLearnerRequestToNextProcessor = Boolean.getBoolean(SKIP_LEARNER_REQUEST_TO_NEXT_PROCESSOR);
        LOG.info("Initialized FollowerRequestProcessor with {} as {}", SKIP_LEARNER_REQUEST_TO_NEXT_PROCESSOR,
                skipLearnerRequestToNextProcessor);
    }

    @Override
    public void run() {
        try {
            while (!finished) {
                ServerMetrics.getMetrics().LEARNER_REQUEST_PROCESSOR_QUEUE_SIZE.add(queuedRequests.size());

                Request request = queuedRequests.take();
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK, 'F', request, "");
                }
                if (request == Request.requestOfDeath) {
                    break;
                }

                // Screen quorum requests against ACLs first
                if (!zks.authWriteRequest(request)) {
                    continue;
                }

                // We want to queue the request to be processed before we submit
                // the request to the leader so that we are ready to receive
                // the response
                maybeSendRequestToNextProcessor(request);

                if (request.isThrottled()) {
                    continue;
                }

                //region 将事务请求和sync请求等转发给leader处理
                // We now ship the request to the leader. As with all
                // other quorum operations, sync also follows this code
                // path, but different from others, we need to keep track
                // of the sync operations this follower has pending, so we
                // add it to pendingSyncs.
                switch (request.type) {
                    case OpCode.sync:
                        zks.pendingSyncs.add(request);
                        zks.getFollower().request(request);
                        break;
                    case OpCode.create:
                    case OpCode.create2:
                    case OpCode.createTTL:
                    case OpCode.createContainer:
                    case OpCode.delete:
                    case OpCode.deleteContainer:
                    case OpCode.setData:
                    case OpCode.reconfig:
                    case OpCode.setACL:
                    case OpCode.multi:
                    case OpCode.check:
                        zks.getFollower().request(request);
                        break;
                    case OpCode.createSession:
                    case OpCode.closeSession:
                        // Don't forward local sessions to the leader.
                        if (!request.isLocalSession()) {
                            zks.getFollower().request(request);
                        }
                        break;
                }
                //endregion
            }
        } catch (RuntimeException e) { // spotbugs require explicit catch of RuntimeException
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("FollowerRequestProcessor exited loop!");
    }

    private void maybeSendRequestToNextProcessor(Request request) throws RequestProcessorException {
        if (skipLearnerRequestToNextProcessor && request.isFromLearner()) {
            ServerMetrics.getMetrics().SKIP_LEARNER_REQUEST_TO_NEXT_PROCESSOR_COUNT.add(1);
        } else {
            nextProcessor.processRequest(request);
        }
    }

    @Override
    public void processRequest(Request request) {
        processRequest(request, true);
    }

    /**
     * 缓存请求及session升级请求等待处理
     */
    void processRequest(Request request, boolean checkForUpgrade/* 来自自客户端请求需升级session */) {
        if (!finished) {

            //region 若需要升级为全局session、为请求创建session创建请求转发给leader处理
            if (checkForUpgrade) {
                // Before sending the request, check if the request requires a
                // global session and what we have is a local session. If so do
                // an upgrade.
                Request upgradeRequest = null;
                try {
                    upgradeRequest = zks.checkUpgradeSession(request);
                } catch (KeeperException ke) {
                    if (request.getHdr() != null) {
                        request.getHdr().setType(OpCode.error);
                        request.setTxn(new ErrorTxn(ke.code().intValue()));
                    }
                    request.setException(ke);
                    LOG.warn("Error creating upgrade request", ke);
                } catch (IOException ie) {
                    LOG.error("Unexpected error in upgrade", ie);
                }
                if (upgradeRequest != null) {
                    queuedRequests.add(upgradeRequest);
                }
            }
            //endregion

            queuedRequests.add(request);
        }
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
