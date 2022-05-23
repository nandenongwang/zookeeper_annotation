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
 * observer处理器 【与follower处理器基本一致】
 * 1、检测升级session
 * 2、转发事务请求给leader处理
 * 3、commit等待leader请求完成通知
 * This RequestProcessor forwards any requests that modify the state of the
 * system to the Leader.
 */
public class ObserverRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ObserverRequestProcessor.class);

    ObserverZooKeeperServer zks;

    RequestProcessor nextProcessor;

    // We keep a queue of requests. As requests get submitted they are
    // stored here. The queue is drained in the run() method.
    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<>();

    boolean finished = false;

    /**
     * Constructor - takes an ObserverZooKeeperServer to associate with
     * and the next processor to pass requests to after we're finished.
     *
     * @param zks
     * @param nextProcessor
     */
    public ObserverRequestProcessor(ObserverZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("ObserverRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
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
                nextProcessor.processRequest(request);

                if (request.isThrottled()) {
                    continue;
                }

                // We now ship the request to the leader. As with all
                // other quorum operations, sync also follows this code
                // path, but different from others, we need to keep track
                // of the sync operations this Observer has pending, so we
                // add it to pendingSyncs.
                switch (request.type) {
                    case OpCode.sync:
                        zks.pendingSyncs.add(request);
                        zks.getObserver().request(request);
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
                        zks.getObserver().request(request);
                        break;
                    case OpCode.createSession:
                    case OpCode.closeSession:
                        // Don't forward local sessions to the leader.
                        if (!request.isLocalSession()) {
                            zks.getObserver().request(request);
                        }
                        break;
                }
            }
        } catch (RuntimeException e) { // spotbugs require explicit catch of RuntimeException
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("ObserverRequestProcessor exited loop!");
    }

    /**
     * Simply queue the request, which will be processed in FIFO order.
     */
    @Override
    public void processRequest(Request request) {
        if (!finished) {
            Request upgradeRequest = null;
            try {
                upgradeRequest = zks.checkUpgradeSession(request);
            } catch (KeeperException ke) {
                if (request.getHdr() != null) {
                    request.getHdr().setType(OpCode.error);
                    request.setTxn(new ErrorTxn(ke.code().intValue()));
                }
                request.setException(ke);
                LOG.info("Error creating upgrade request", ke);
            } catch (IOException ie) {
                LOG.error("Unexpected error in upgrade", ie);
            }
            if (upgradeRequest != null) {
                queuedRequests.add(upgradeRequest);
            }
            queuedRequests.add(request);
        }
    }

    /**
     * Shutdown the processor.
     */
    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
