package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;

/**
 * 事件
 * A WatchedEvent represents a change on the ZooKeeper that a Watcher
 * is able to respond to.  The WatchedEvent includes exactly what happened,
 * the current state of the ZooKeeper, and the path of the znode that
 * was involved in the event.
 */
@InterfaceAudience.Public
public class WatchedEvent {

    /**
     * 连接状态
     */
    private final KeeperState keeperState;

    /**
     * 事件类型
     */
    private final EventType eventType;

    /**
     * 事件产生路径
     */
    private final String path;

    /**
     * Create a WatchedEvent with specified type, state and path
     */
    public WatchedEvent(EventType eventType, KeeperState keeperState, String path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
    }

    /**
     * Convert a WatcherEvent sent over the wire into a full-fledged WatcherEvent
     */
    public WatchedEvent(WatcherEvent eventMessage) {
        keeperState = KeeperState.fromInt(eventMessage.getState());
        eventType = EventType.fromInt(eventMessage.getType());
        path = eventMessage.getPath();
    }

    public KeeperState getState() {
        return keeperState;
    }

    public EventType getType() {
        return eventType;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "WatchedEvent state:" + keeperState + " type:" + eventType + " path:" + path;
    }

    /**
     * Convert WatchedEvent to type that can be sent over network
     */
    public WatcherEvent getWrapper() {
        return new WatcherEvent(eventType.getIntValue(), keeperState.getIntValue(), path);
    }

}
