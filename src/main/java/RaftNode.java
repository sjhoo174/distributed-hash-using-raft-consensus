import java.io.IOException;
import java.lang.Thread;
import java.net.*;
import java.util.*;

public class RaftNode {
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    public enum Phase {
        ZERO,
        ONE,
        TWO
    }
    public int currentTerm;
    public int votedFor;
    public int numVotes;
    public int majority;
    public int leaderId;
    public Role role;
    public Phase phase;
    public int newVotes;
    public int oldVotes;
    public ArrayList<Integer> newIds;
    public ArrayList<Integer> oldIds;
    public Thread election;
    public Thread followerState;
    public Thread leaderState;
    public ArrayList<Integer> peers;
    public ArrayList<Integer> requestVotes;
    public int id;
    public int lastLogTerm;
    public int lastLogIndex;
    public long lastUpdate;
    public HashMap<Integer, Integer> nextIndex;
    public HashMap<Integer, Integer> matchIndex;
    public int commitIndex;
    public ArrayList<Command.LogEntry> log;

    public class Address {
        String host;
        int port;
    }
    public HashMap<Integer, Address> addressBook;
    public int port;

    public Thread listener;
    public Acceptor followerManager;

    public RaftNode() {
        this.followerManager = new Acceptor();
    }

    public double generateRandomTimeout() {
        Random random = new Random();
        return 5 * random.nextDouble() * 5;
    }

    public void follower() {
        this.role = Role.FOLLOWER;
        this.lastUpdate = System.currentTimeMillis();
        double electionTimeout = generateRandomTimeout();
        while (System.currentTimeMillis() - this.lastUpdate <= electionTimeout) {};
        this.startElection();
        while(true) {
            this.lastUpdate = System.currentTimeMillis();
            electionTimeout = generateRandomTimeout();
            while (System.currentTimeMillis() - this.lastUpdate <= electionTimeout) {};
            if (this.election.isAlive()) {
                this.election.interrupt();
            }
            this.startElection();
        }
    }

    public Thread createFollowerThread() {
        return new Thread(this::follower);
    }

    public void listen(Acceptor acceptor) {
        try(DatagramSocket socket = new DatagramSocket(this.port)) {
            System.out.println("started listening...");
            byte[] receiveData = new byte[1024];
            while (true) {
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                socket.receive(receivePacket);
                byte[] data = receivePacket.getData();
                String host = receivePacket.getAddress().getHostAddress();
                int port = receivePacket.getPort();
                Thread t = new Thread(() -> acceptor.accept(RaftNode.this, data, host, port));
                t.start();
            }
        } catch (SocketException e) {

        } catch (IOException e) {

        }
    }

    public void startElection() {
        this.role = Role.CANDIDATE;
        this.election = new Thread(new Runnable() {
            @Override
            public void run() {
                RaftNode.this.role = Role.CANDIDATE;
                RaftNode.this.requestVotes = new ArrayList<>(RaftNode.this.peers);
                int sender = RaftNode.this.id;
                while(true) {
                    for (int peer: RaftNode.this.peers) {
                        if (RaftNode.this.peers.contains(peer)) {
                            String msg = RaftNode.this.lastLogTerm + " " + RaftNode.this.lastLogIndex;
                            Command.RequestVoteMsg requestVoteMsg =  new Command.RequestVoteMsg(sender, peer, RaftNode.this.currentTerm, msg);

                            byte[] bytes;
                            try {
                                bytes = Format.msgToBytes(requestVoteMsg);
                            } catch (IOException e) {
                                e.printStackTrace();
                                return;
                            }

                            try (DatagramSocket socket = new DatagramSocket()) {
                                Address address = RaftNode.this.addressBook.get(peer);
                                DatagramPacket packet = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(address.host), address.port);
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
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        if (!this.peers.isEmpty()) {
            this.currentTerm += 1;
            this.votedFor = this.id;
            this.numVotes = 1;
            if (this.phase == Phase.ONE) {
                this.newVotes = 0;
                this.oldVotes = 0;
                if (this.newIds.contains(this.id)) {
                    this.newVotes = 1;
                }
                if (this.oldIds.contains(this.id)) {
                    this.oldVotes = 1;
                }
            } else if (this.phase == Phase.TWO) {
                this.newVotes = 0;
                if (this.newIds.contains(this.id)) {
                    this.newVotes = 1;
                }
            }
            this.election.start();
        }
    }

    public void appendEntries() {
        ArrayList<Integer> receipts;
        while(true) {
            receipts = new ArrayList<>(this.peers);
            if (this.phase != null) {
                for (int peer: this.peers) {
                    if (!this.nextIndex.containsKey(peer)) {
                        this.nextIndex.put(peer, this.log.size() + 1);
                        this.matchIndex.put(peer, 0);
                    }
                }
            }

            for (int peer: receipts) {
                int prevLogTerm;
                int prevLogIndex;
                ArrayList<Command.LogEntry> entries;
                if (this.log.size() >= this.nextIndex.get(peer)) {
                    prevLogIndex = this.nextIndex.get(peer) - 1;
                    if (prevLogIndex != 0) {
                        prevLogTerm = this.log.get(prevLogIndex - 1).term;
                    } else {
                        prevLogTerm = 0;
                    }
                    entries = new ArrayList<Command.LogEntry>(Collections.singletonList(this.log.get(this.nextIndex.get(peer) - 1)));
                } else {
                    entries = new ArrayList<>();
                    prevLogIndex = this.log.size();
                    if (prevLogIndex != 0) {
                        prevLogTerm = this.log.get(prevLogIndex-1).term;
                    } else {
                        prevLogTerm = 0;
                    }
                }

                Command.AppendEntriesMsg msg = new Command.AppendEntriesMsg(this.id, peer, this.currentTerm, entries, this.commitIndex, prevLogIndex, prevLogTerm);
                byte[] serializedData = new byte[0];
                try {
                    serializedData = Format.msgToBytes(msg);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                try (DatagramSocket socket = new DatagramSocket()) {
                    if (!this.addressBook.containsKey(peer)) {
                        System.out.println("Peer not found in address book " + peer);
                        return;
                    }
                    Address address = this.addressBook.get(peer);
                    DatagramPacket packet = new DatagramPacket(serializedData, serializedData.length, InetAddress.getByName(address.host), address.port);
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

            try {
                Thread.sleep(500);
            } catch(InterruptedException e) {
                e.printStackTrace();
                return;
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }

    public void leader() {
        System.out.println("Running as leader");
        this.role = Role.LEADER;
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        for (int peer: this.peers) {
            this.nextIndex.put(peer, this.log.size()+1);
            this.matchIndex.put(peer, 0);
        }
        this.appendEntries();
    }


    public Thread createLeaderThread() {
        return new Thread(this::leader);
    }

    public void stepDown() {
        if (this.role == Role.CANDIDATE) {
            this.election.interrupt();
            this.lastUpdate = System.currentTimeMillis();
            this.role = Role.FOLLOWER;
        } else if (this.role == Role.LEADER) {
            this.leaderState.interrupt();
            this.followerState = createFollowerThread();
        }
    }

    public void save() {

    }

    public void load() {

    }

    public void run() {
        try {
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        this.followerState = createFollowerThread();
    }
}
