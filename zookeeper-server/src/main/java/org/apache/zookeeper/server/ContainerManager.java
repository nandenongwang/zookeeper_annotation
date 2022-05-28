package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 容器节点与可过期临时节点检测删除线程
 * 容器节点：无子节点、新创建节点可等待一定时间后删除
 * 可过期临时节点：已过过期时间
 * Manages cleanup of container ZNodes. This class is meant to only
 * be run from the leader. There's no harm in running from followers/observers
 * but that will be extra work that's not needed. Once started, it periodically
 * checks container nodes that have a cversion &gt; 0 and have no children. A
 * delete is attempted on the node. The result of the delete is unimportant.
 * If the proposal fails or the container node is not empty there's no harm.
 */
public class ContainerManager {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
    private final ZKDatabase zkDb;

    /**
     * 处理链 【删除命令会交由处理链按事务提案执行】
     */
    private final RequestProcessor requestProcessor;

    /**
     * 检测任务执行周期
     */
    private final int checkIntervalMs;

    /**
     * 每分钟最大运行次数
     */
    private final int maxPerMinute;

    /**
     * 未变更新容器节点最大删除等待时间
     */
    private final long maxNeverUsedIntervalMs;
    private final Timer timer;
    private final AtomicReference<TimerTask> task = new AtomicReference<>(null);

    /**
     * @param zkDb             the ZK database
     * @param requestProcessor request processer - used to inject delete
     *                         container requests
     * @param checkIntervalMs  how often to check containers in milliseconds
     * @param maxPerMinute     the max containers to delete per second - avoids
     *                         herding of container deletions
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor, int checkIntervalMs, int maxPerMinute) {
        this(zkDb, requestProcessor, checkIntervalMs, maxPerMinute, 0);
    }

    /**
     * @param zkDb                   the ZK database
     * @param requestProcessor       request processer - used to inject delete
     *                               container requests
     * @param checkIntervalMs        how often to check containers in milliseconds
     * @param maxPerMinute           the max containers to delete per second - avoids
     *                               herding of container deletions
     * @param maxNeverUsedIntervalMs the max time in milliseconds that a container that has never had
     *                               any children is retained
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor, int checkIntervalMs, int maxPerMinute, long maxNeverUsedIntervalMs) {
        this.zkDb = zkDb;
        this.requestProcessor = requestProcessor;
        this.checkIntervalMs = checkIntervalMs;
        this.maxPerMinute = maxPerMinute;
        this.maxNeverUsedIntervalMs = maxNeverUsedIntervalMs;
        timer = new Timer("ContainerManagerTask", true);

        LOG.info("Using checkIntervalMs={} maxPerMinute={} maxNeverUsedIntervalMs={}", checkIntervalMs, maxPerMinute, maxNeverUsedIntervalMs);
    }

    /**
     * start/restart the timer the runs the check. Can safely be called
     * multiple times.
     */
    public void start() {
        if (task.get() == null) {
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        checkContainers();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("interrupted");
                        cancel();
                    } catch (Throwable e) {
                        LOG.error("Error checking containers", e);
                    }
                }
            };
            if (task.compareAndSet(null, timerTask)) {
                timer.scheduleAtFixedRate(timerTask, checkIntervalMs, checkIntervalMs);
            }
        }
    }

    /**
     * stop the timer if necessary. Can safely be called multiple times.
     */
    public void stop() {
        TimerTask timerTask = task.getAndSet(null);
        if (timerTask != null) {
            timerTask.cancel();
        }
        timer.cancel();
    }

    /**
     * Manually check the containers. Not normally used directly
     */
    public void checkContainers() throws InterruptedException {
        long minIntervalMs = getMinIntervalMs();
        for (String containerPath : getCandidates()) {
            long startMs = Time.currentElapsedTime();

            ByteBuffer path = ByteBuffer.wrap(containerPath.getBytes(UTF_8));
            Request request = new Request(null, 0, 0, ZooDefs.OpCode.deleteContainer, path, null);
            try {
                LOG.info("Attempting to delete candidate container: {}", containerPath);
                postDeleteRequest(request);
            } catch (Exception e) {
                LOG.error("Could not delete container: {}", containerPath, e);
            }

            long elapsedMs = Time.currentElapsedTime() - startMs;
            long waitMs = minIntervalMs - elapsedMs;
            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }
        }
    }

    // VisibleForTesting
    protected void postDeleteRequest(Request request) throws RequestProcessor.RequestProcessorException {
        requestProcessor.processRequest(request);
    }

    // VisibleForTesting
    protected long getMinIntervalMs() {
        return TimeUnit.MINUTES.toMillis(1) / maxPerMinute;
    }

    // VisibleForTesting

    /**
     * 查找可删除节点路径 【包括容器节点与已过期的临时节点】
     */
    protected Collection<String> getCandidates() {
        Set<String> candidates = new HashSet<>();
        //region 查找可删除的容器节点
        for (String containerPath : zkDb.getDataTree().getContainers()) {
            DataNode node = zkDb.getDataTree().getNode(containerPath);
            if ((node != null) && node.getChildren().isEmpty()) {
                //防止刚创建就正好被删除、没有变更过需要至少等待maxNeverUsedIntervalMs
                /*
                    cversion > 0: keep newly created containers from being deleted
                    before any children have been added. If you were to create the
                    container just before a container cleaning period the container
                    would be immediately be deleted.
                 */
                if (node.stat.getCversion() > 0) {
                    candidates.add(containerPath);
                } else {
                    /*
                        Users may not want unused containers to live indefinitely. Allow a system
                        property to be set that sets the max time for a cversion-0 container
                        to stay before being deleted
                     */
                    if ((maxNeverUsedIntervalMs != 0) && (getElapsed(node) > maxNeverUsedIntervalMs)) {
                        candidates.add(containerPath);
                    }
                }
            }
        }
        //endregion

        //region 查找可删除的可过期临时节点
        for (String ttlPath : zkDb.getDataTree().getTtls()) {
            DataNode node = zkDb.getDataTree().getNode(ttlPath);
            if (node != null) {
                Set<String> children = node.getChildren();
                if (children.isEmpty()) {
                    if (EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL) {
                        long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
                        if ((ttl != 0) && (getElapsed(node) > ttl)) {
                            candidates.add(ttlPath);
                        }
                    }
                }
            }
        }
        //endregion

        return candidates;
    }

    // VisibleForTesting
    protected long getElapsed(DataNode node) {
        return Time.currentWallTime() - node.stat.getMtime();
    }

}
