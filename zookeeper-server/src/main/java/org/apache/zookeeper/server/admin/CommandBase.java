package org.apache.zookeeper.server.admin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class CommandBase implements Command {

    private final String primaryName;
    private final Set<String> names;
    private final String doc;
    private final boolean serverRequired;

    /**
     * @param names The possible names of this command, with the primary name first.
     */
    protected CommandBase(List<String> names) {
        this(names, true, null);
    }
    protected CommandBase(List<String> names, boolean serverRequired) {
        this(names, serverRequired, null);
    }

    protected CommandBase(List<String> names, boolean serverRequired, String doc) {
        this.primaryName = names.get(0);
        this.names = new HashSet<>(names);
        this.doc = doc;
        this.serverRequired = serverRequired;
    }

    @Override
    public String getPrimaryName() {
        return primaryName;
    }

    @Override
    public Set<String> getNames() {
        return names;
    }

    @Override
    public String getDoc() {
        return doc;
    }

    @Override
    public boolean isServerRequired() {
        return serverRequired;
    }

    /**
     * @return A response with the command set to the primary name and the
     *         error set to null (these are the two entries that all command
     *         responses are required to include).
     */
    protected CommandResponse initializeResponse() {
        return new CommandResponse(primaryName);
    }

}
