package org.apache.zookeeper.cli;

import java.util.function.Supplier;

/**
 * 所有命令行命令
 * Factory class for creating instances of {@link CliCommand}.
 */
public class CommandFactory {

    /**
     * All Cli Commands.
     */
    public enum Command {
        CLOSE(CloseCommand::new),
        CREATE(CreateCommand::new),
        DELETE(DeleteCommand::new),
        DELETE_ALL(DeleteAllCommand::new),
        SET(SetCommand::new),
        GET(GetCommand::new),
        LS(LsCommand::new),
        GET_ACL(GetAclCommand::new),
        SET_ACL(SetAclCommand::new),
        STAT(StatCommand::new),
        SYNC(SyncCommand::new),
        SET_QUOTA(SetQuotaCommand::new),
        LIST_QUOTA(ListQuotaCommand::new),
        DEL_QUOTA(DelQuotaCommand::new),
        ADD_AUTH(AddAuthCommand::new),
        RECONFIG(ReconfigCommand::new),
        GET_CONFIG(GetConfigCommand::new),
        REMOVE_WATCHES(RemoveWatchesCommand::new),
        GET_EPHEMERALS(GetEphemeralsCommand::new),
        GET_ALL_CHILDREN_NUMBER(GetAllChildrenNumberCommand::new),
        VERSION(VersionCommand::new),
        ADD_WATCH(AddWatchCommand::new),
        WHO_AM_I(WhoAmICommand::new);

        private final Supplier<? extends CliCommand> instantiator;

        private CliCommand getInstance() {
            return instantiator.get();
        }

        Command(Supplier<? extends CliCommand> instantiator) {
            this.instantiator = instantiator;
        }
    }

    /**
     * Creates a new {@link CliCommand} instance.
     *
     * @param command the {@link Command} to create a new instance of
     * @return the new {@code CliCommand} instance
     */
    public static CliCommand getInstance(Command command) {
        return command.getInstance();
    }
}
