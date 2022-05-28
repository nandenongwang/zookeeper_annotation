package org.apache.zookeeper;

import java.util.Collections;
import java.util.List;
import jline.console.completer.Completer;

class JLineZNodeCompleter implements Completer {

    private ZooKeeper zk;

    public JLineZNodeCompleter(ZooKeeper zk) {
        this.zk = zk;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public int complete(String buffer, int cursor, List candidates) {
        // Guarantee that the final token is the one we're expanding
        buffer = buffer.substring(0, cursor);
        String token = "";
        if (!buffer.endsWith(" ")) {
            String[] tokens = buffer.split(" ");
            if (tokens.length != 0) {
                token = tokens[tokens.length - 1];
            }
        }

        if (token.startsWith("/")) {
            return completeZNode(buffer, token, candidates);
        }
        return completeCommand(buffer, token, candidates);
    }

    private int completeCommand(String buffer, String token, List<String> candidates) {
        for (String cmd : ZooKeeperMain.getCommands()) {
            if (cmd.startsWith(token)) {
                candidates.add(cmd);
            }
        }
        return buffer.lastIndexOf(" ") + 1;
    }

    private int completeZNode(String buffer, String token, List<String> candidates) {
        String path = token;
        int idx = path.lastIndexOf("/") + 1;
        String prefix = path.substring(idx);
        try {
            // Only the root path can end in a /, so strip it off every other prefix
            String dir = idx == 1 ? "/" : path.substring(0, idx - 1);
            List<String> children = zk.getChildren(dir, false);
            for (String child : children) {
                if (child.startsWith(prefix)) {
                    candidates.add(child);
                }
            }
        } catch (InterruptedException e) {
            return 0;
        } catch (KeeperException e) {
            return 0;
        }
        Collections.sort(candidates);
        return candidates.size() == 0 ? buffer.length() : buffer.lastIndexOf("/") + 1;
    }

}
