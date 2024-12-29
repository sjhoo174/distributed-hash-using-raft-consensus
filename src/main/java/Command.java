import java.util.ArrayList;

public class Command {
    public enum Type {
        APPEND_ENTRIES,
        REQUEST_VOTE,
        REQUEST_VOTE_RESPONSE,
        APPEND_ENTRIES_RESPONSE,
        CLIENT,
        REDIRECT,
        CONFIG_CHANGE
    }

    public static class BaseCommand {
        Type type;

        public BaseCommand(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }
    }

    public static class BaseMsg extends BaseCommand {
        int sender, receiver, term;

        public BaseMsg(int sender, int receiver, int term, Type type) {
            super(type);
            this.sender = sender;
            this.receiver = receiver;
            this.term = term;
        }

    }

    public static class RequestVoteMsg extends BaseMsg {
        String data;
        public RequestVoteMsg(int sender, int receiver, int term, String data) {
            super(sender, receiver, term, Type.REQUEST_VOTE);
            this.data = data;
        }
    }

    public static class VoteResponseMsg extends BaseMsg {
        String data;
        public VoteResponseMsg(int sender, int receiver, int term, String data) {
            super(sender, receiver, term, Type.REQUEST_VOTE_RESPONSE);
            this.data = data;
        }
    }

    public static class AppendEntriesMsg extends BaseMsg {
        ArrayList<LogEntry> entries;
        int commitIndex;
        int prevLogTerm;
        int prevLogIndex;

        public AppendEntriesMsg(int sender, int receiver, int term, ArrayList<LogEntry> entries, int commitIndex, int prevLogIndex, int prevLogTerm) {
            super(sender, receiver, term, Type.APPEND_ENTRIES);
            this.entries = entries;
            this.commitIndex = commitIndex;
            this.prevLogTerm = prevLogTerm;
            this.prevLogIndex = prevLogIndex;
        }
    }

    public static class AppendEntriesResponseMsg extends BaseMsg {
        String success;
        int matchIndex;

        public AppendEntriesResponseMsg(int sender, int receiver, int term, String success, int matchIndex) {
            super(sender, receiver, term, Type.APPEND_ENTRIES_RESPONSE);
            this.success = success;
            this.matchIndex = matchIndex;
        }
    }


    public static class LogEntry {
        int term;
        BaseCommand command;
        String uuid;
        String address;
        RaftNode.Phase phase;

        public LogEntry(int term, BaseCommand command, String uuid, String address, RaftNode.Phase phase) {
            this.term = term;
            this.command = command;
            this.uuid = uuid;
            this.address = address;
            this.phase = phase;
        }
    }

    public static class ClientRequestMsg extends BaseCommand {
        String msg;
        String uuid;

        public ClientRequestMsg(String msg, String uuid) {
            super( Type.CLIENT);
            this.msg = msg;
            this.uuid = uuid;
        }

        public ClientRequestMsg(String msg, String uuid, Type type) {
            super(type);
            this.msg = msg;
            this.uuid = uuid;
        }
    }

    public static class RequestRedirectMsg extends ClientRequestMsg {
        String address;
        public RequestRedirectMsg(String msg, String uuid, String address) {
            super(msg, uuid, Type.REDIRECT);
            this.address = address;
        }
    }

    public static class ConfigChangeMsg extends BaseCommand {
        ArrayList<Integer> newIds;
        RaftNode.Phase phase;
        String address = null;
        String uuid;

        public ConfigChangeMsg(ArrayList<Integer> newConfig, String uuid, String address, RaftNode.Phase phase) {
            super( Type.CONFIG_CHANGE);
            this.uuid = uuid;
            this.newIds = newConfig;
            this.address = address;
            this.phase = phase;
        }
    }


    public static class ServerConfig {
        int currentTerm;
        int votedFor;
        BaseCommand[] log;
        int[] peers;

        public ServerConfig(int currentTerm, int votedFor, BaseCommand[] log, int[] peers) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.log = log;
            this.peers = peers;
        }
    }

}
