package org.apache.zookeeper;

/**
 * Abstraction that exposes various methods useful for testing ZooKeeper
 */
public interface Testable {

    /**
     * Cause the ZooKeeper instance to behave as if the session expired
     */
    void injectSessionExpiration();

    /**
     * Allow an event to be inserted into the event queue
     *
     * @param event event to insert
     */
    void queueEvent(WatchedEvent event);

}
