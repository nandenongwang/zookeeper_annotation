package org.apache.zookeeper.server.admin;

import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Interface implemented by all commands runnable by JettyAdminServer.
 *
 * @see CommandBase
 * @see Commands
 * @see JettyAdminServer
 */
public interface Command {

    /**
     * The set of all names that can be used to refer to this command (e.g.,
     * "configuration", "config", and "conf").
     */
    Set<String> getNames();

    /**
     * The name that is returned with the command response and that appears in
     * the list of all commands. This should be a member of the set returned by
     * getNames().
     */
    String getPrimaryName();

    /**
     * A string documenting this command (e.g., what it does, any arguments it
     * takes).
     */
    String getDoc();

    /**
     * @return true if the command requires an active ZooKeeperServer or a
     *     synced peer in order to resolve
     */
    boolean isServerRequired();

    /**
     * Run this command. Commands take a ZooKeeperServer and String-valued
     * keyword arguments and return a map containing any information
     * constituting the response to the command. Commands are responsible for
     * parsing keyword arguments and performing any error handling if necessary.
     * Errors should be reported by setting the "error" entry of the returned
     * map with an appropriate message rather than throwing an exception.
     *
     * @param zkServer
     * @param kwargs keyword -&gt; argument value mapping
     * @return Map representing response to command containing at minimum:
     *    - "command" key containing the command's primary name
     *    - "error" key containing a String error message or null if no error
     */
    CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs);

}
