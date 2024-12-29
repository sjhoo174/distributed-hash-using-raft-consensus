import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class Acceptor {

    public void accept(RaftNode server, byte[] data, String host, int port) {
        Command.BaseCommand entry;
        try {
            entry = Format.bytesToEntry(data);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
  
        Command.Type entryType = entry.getType();

        switch (entryType) {

            case CONFIG_CHANGE -> {
                Command.ConfigChangeMsg configChangeMsg = (Command.ConfigChangeMsg) entry;
                if (configChangeMsg.phase == RaftNode.Phase.ONE) {
                    System.out.println("config change phase one");
                    server.phase = RaftNode.Phase.ONE;
                    server.newIds = configChangeMsg.newIds;
                    server.oldIds = new ArrayList<>(server.peers);
                    server.oldIds.add(server.id);
                    String address = "";
                    if (configChangeMsg.address != null) {
                        address = configChangeMsg.address;
                    }

                    Command.LogEntry newEntry = new Command.LogEntry(server.currentTerm, configChangeMsg, address, configChangeMsg.uuid, RaftNode.Phase.ONE);
                    server.log.add(newEntry);
                    server.peers = new ArrayList<>(Stream.concat(server.oldIds.stream(), server.newIds.stream()).toList());
                    server.peers.remove(server.id);
                    server.save();
                    System.out.println("server config phase 1 applied");

                } else {
                    System.out.println("config change phase two");
                    server.phase = RaftNode.Phase.TWO;
                    server.newIds = configChangeMsg.newIds;
                    String address = "";
                    if (configChangeMsg.address != null) {
                        address = configChangeMsg.address;
                    }

                    Command.LogEntry newEntry = new Command.LogEntry(server.currentTerm, configChangeMsg, address, configChangeMsg.uuid, RaftNode.Phase.TWO);
                    server.log.add(newEntry);
                    server.peers = new ArrayList<>(server.newIds);
                    if (server.peers.contains(server.id)) {
                        server.peers.remove(server.id);
                    }
                    server.save();
                    System.out.println("server config phase 2 applied, running peers");
                }

                if (server.role != RaftNode.Role.LEADER) {
                    System.out.println("redirecting config change to leader");
                    int redirectTarget;
                    if (server.leaderId != 0) {
                        redirectTarget = server.leaderId;
                    } else {
                        Random random = new Random();
                        int randomIdx = random.nextInt(server.peers.size());
                        int randomPeerId = server.peers.get(randomIdx);
                        redirectTarget = randomPeerId;
                    }
                    String address = "";
                    if (configChangeMsg.address != null) {
                        address = configChangeMsg.address;
                    }
                    Command.ConfigChangeMsg redirectMsg = new Command.ConfigChangeMsg(configChangeMsg.newIds, configChangeMsg.uuid, address, configChangeMsg.phase);
                    byte[] serializedData = new byte[0];
                    try {
                        serializedData = Format.msgToBytes(redirectMsg);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }

                    try (DatagramSocket socket = new DatagramSocket()) {
                        RaftNode.Address addr = server.addressBook.get(redirectTarget);
                        DatagramPacket packet = new DatagramPacket(serializedData, serializedData.length, InetAddress.getByName(addr.host), addr.port);
                        socket.send(packet);
                    } catch(SocketException e) {
                        e.printStackTrace();
                        return;
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        return;
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }

            case REQUEST_VOTE -> {
                Command.RequestVoteMsg requestVoteMsg = (Command.RequestVoteMsg) entry;

                int sender = requestVoteMsg.sender;
                int term = requestVoteMsg.term;

                if (!server.peers.contains(sender)) {
                    return;
                }
                String msgStr = new String(data);
                String[] words = msgStr.split(" ");
                int logTerm = Integer.parseInt(words[0]);
                int logIdx = Integer.parseInt(words[1]);
                int voteGranted;
                if (term < server.currentTerm) {
                    System.out.println("rejected due to old term");
                    voteGranted = 0;
                } else if (term == server.currentTerm) {
                    if ((logTerm >= server.lastLogTerm && logIdx >= server.lastLogIndex) && (server.votedFor == -1 || server.votedFor == sender)) {
                        voteGranted = 1;
                        server.votedFor = sender;
                        server.save();
                    } else {
                        voteGranted = 0;
                    }
                } else {
                    // find higher term
                    server.currentTerm = term;
                    server.save();
                    server.stepDown();
                    if (logTerm >= server.lastLogTerm && logIdx >= server.lastLogIndex) {
                        voteGranted = 1;
                        server.votedFor = sender;
                        server.save();
                    } else {
                        voteGranted = 0;
                    }
                }

                String reply = String.valueOf(voteGranted);
                Command.VoteResponseMsg replyMsg = new Command.VoteResponseMsg(server.id, sender, server.currentTerm, reply);
                byte[] serializedData;
                try {
                    serializedData = Format.msgToBytes(replyMsg);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                try (DatagramSocket socket = new DatagramSocket()) {
                    DatagramPacket packet = new DatagramPacket(serializedData, serializedData.length, InetAddress.getByName(host), port);
                    socket.send(packet);
                } catch(SocketException e) {
                    e.printStackTrace();
                    return;
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }

            case REQUEST_VOTE_RESPONSE -> {
                Command.VoteResponseMsg voteResponseMsg = (Command.VoteResponseMsg) entry;

                int sender = voteResponseMsg.sender;
                int term = voteResponseMsg.term;

                ByteBuffer buffer = ByteBuffer.wrap(data);
                int voteGranted = buffer.getInt();
                if (voteGranted == 1) {
                    if (server.phase == RaftNode.Phase.ZERO) {
                        if (server.role == RaftNode.Role.CANDIDATE) {
                            server.requestVotes.remove(sender);
                            server.numVotes += 1;
                            if (server.numVotes == server.majority) {
                                System.out.printf("get majority votes, become leader at term %d\n", server.currentTerm);
                                if (server.election.isAlive()) {
                                    server.election.interrupt();
                                }

                                server.role = RaftNode.Role.LEADER;
                                server.followerState.interrupt();
                                server.leaderState = server.createLeaderThread();
                            }
                        }
                    } else if (server.phase == RaftNode.Phase.ONE) {
                        server.requestVotes.remove(sender);
                        if (server.oldIds.contains(sender)) {
                            server.oldVotes += 1;
                        }
                        if (server.newIds.contains(sender)) {
                            server.newVotes += 1;
                        }
                        int majority_one = server.oldIds.size() / 2 + 1;
                        int majority_two = server.newIds.size() / 2 + 1;
                        if (server.oldVotes >= majority_one && server.newVotes >= majority_two) {
                            System.out.printf("get majority votes from old and new, become leader at term %d\n", server.currentTerm);
                            if (server.election.isAlive()) {
                                server.election.interrupt();
                            }

                            server.role = RaftNode.Role.LEADER;
                            server.followerState.interrupt();
                            server.leaderState = server.createLeaderThread();
                        }

                    } else {
                        server.requestVotes.remove(sender);
                        if (server.peers.contains(sender)) {
                            server.newVotes += 1;
                        }
                        int majority = server.newIds.size() / 2 + 1;
                        if (server.newVotes >= majority) {
                            System.out.printf("get majority votes from new, become leader at term %d\n", server.currentTerm);

                            if (server.election.isAlive()) {
                                server.election.interrupt();
                            }

                            server.role = RaftNode.Role.LEADER;
                            server.followerState.interrupt();
                            server.leaderState = server.createLeaderThread();
                        }
                    }
                } else {
                    if (term > server.currentTerm) {
                        server.currentTerm = term;
                        server.save();
                        if (server.role == RaftNode.Role.CANDIDATE) {
                            server.stepDown();
                        }
                    }
                    System.out.printf("vote rejected by %d\n", sender);
                }
            }

            case APPEND_ENTRIES -> {
                Command.AppendEntriesMsg appendEntriesMsg = (Command.AppendEntriesMsg) entry;
                int term = appendEntriesMsg.term;
                int sender = appendEntriesMsg.sender;

                ArrayList<Command.LogEntry> entries = appendEntriesMsg.entries;
                int commitIndex = appendEntriesMsg.commitIndex;
                int prevLogTerm = appendEntriesMsg.prevLogTerm;
                int prevLogIndex = appendEntriesMsg.prevLogIndex;
                int matchIndex = server.commitIndex;

                String success;
                if (term >= server.currentTerm) {
                    server.currentTerm = term;
                    server.save();
                    server.stepDown();
                    if (server.role == RaftNode.Role.FOLLOWER) {
                        server.lastUpdate = System.currentTimeMillis();
                    }
                    if (prevLogIndex != 0) {
                        if (server.log.size() >= prevLogIndex) {
                            if (server.log.get(prevLogIndex-1).term == prevLogTerm) {
                                success = "true";
                                server.leaderId = sender;
                                if (entries.size() > 0) {
                                    List<Command.LogEntry> updatedLog = new ArrayList<>(server.log.subList(0, prevLogIndex));
                                    updatedLog.addAll(entries);
                                    server.log = entries;
                                    Command.LogEntry logEntry = entries.getFirst();
                                    Command.ConfigChangeMsg configChangeMsg = (Command.ConfigChangeMsg) logEntry.command;
                                    if (logEntry.phase == RaftNode.Phase.ONE) {
                                        server.phase = RaftNode.Phase.ONE;
                                        server.newIds = new ArrayList<>(configChangeMsg.newIds);
                                        server.oldIds = new ArrayList<>(server.peers);
                                        server.oldIds.add(server.id);
                                        server.peers = new ArrayList<>(Stream.concat(server.oldIds.stream(), server.newIds.stream()).toList());
                                        server.peers.remove(server.id);
                                    } else if (logEntry.phase == RaftNode.Phase.TWO) {
                                        server.phase = RaftNode.Phase.TWO;
                                        server.newIds = new ArrayList<>(configChangeMsg.newIds);
                                        server.peers = new ArrayList<>(server.newIds);
                                        server.peers.remove(server.id);
                                        System.out.println("follower applied new config, running peers");
                                    server.save();
                                    }
                                }
                            } else {
                                success = "false";
                            }
                        } else {
                            success = "false";
                        }
                    } else {
                        success = "true";
                        if (entries.size() > 0) {
                            List<Command.LogEntry> updatedLog = new ArrayList<>(server.log.subList(0, prevLogIndex));
                            updatedLog.addAll(entries);
                            server.log = entries;
                            Command.LogEntry logEntry = entries.getFirst();
                            Command.ConfigChangeMsg configChangeMsg = (Command.ConfigChangeMsg) logEntry.command;
                            if (logEntry.phase == RaftNode.Phase.ONE) {
                                server.phase = RaftNode.Phase.ONE;
                                server.newIds = new ArrayList<>(configChangeMsg.newIds);
                                server.oldIds = new ArrayList<>(server.peers);
                                server.oldIds.add(server.id);
                                server.peers = new ArrayList<>(Stream.concat(server.oldIds.stream(), server.newIds.stream()).toList());
                                server.peers.remove(server.id);
                            } else if (logEntry.phase == RaftNode.Phase.TWO) {
                                server.phase = RaftNode.Phase.TWO;
                                server.newIds = new ArrayList<>(configChangeMsg.newIds);
                                server.peers = new ArrayList<>(server.newIds);
                                server.peers.remove(server.id);
                                System.out.println("follower applied new config, running peers");
                            }
                            server.save();
                            matchIndex = server.log.size();
                        }
                        server.leaderId = sender;
                    }
                } else {
                    success = "false";
                }

                Command.AppendEntriesResponseMsg replyMsg = new Command.AppendEntriesResponseMsg(server.id, sender, server.currentTerm, success, matchIndex);
                byte[] serializedData;
                try {
                    serializedData = Format.msgToBytes(replyMsg);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                try (DatagramSocket socket = new DatagramSocket()) {
                    DatagramPacket packet = new DatagramPacket(serializedData, serializedData.length, InetAddress.getByName(host), port);
                    socket.send(packet);
                } catch(SocketException e) {
                    e.printStackTrace();
                    return;
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }

            case APPEND_ENTRIES_RESPONSE -> {

            }

            case CLIENT -> {

            }

            case REDIRECT -> {

            }

        }

    }
}
