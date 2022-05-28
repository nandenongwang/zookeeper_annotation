package org.apache.zookeeper;

/**
 * This interface is used to notify the digest mismatch event.
 */
public interface DigestWatcher {

    /**
     * Called when the digest mismatch is found on a given zxid.
     *
     * @param mismatchZxid the zxid when the digest mismatch happened.
     */
    void process(long mismatchZxid);

}
