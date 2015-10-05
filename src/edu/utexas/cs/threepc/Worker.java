package edu.utexas.cs.threepc;

import edu.utexas.cs.netutil.*;

import java.io.*;
import java.util.*;

/**
 * Note: maybe use sock.connect to set a timeout.
 */
public class Worker {

    private int  processId;
    private int  totalProcess;
    private int  leader;
    private int  viewNumber;
    private File DTLog;
    private int  messageCounter;
    private Map<String, String> playlist;

    private String currentCommand;

    private Boolean[] voteStats;
    private int voteAgreeNum;
    private Boolean[] ackStats;
    private int ackNum;
    private Boolean[] processAlive;
    private int aliveProcessNum;

    private String hostName;
    private int    basePort;
    private int    reBuild;
    private NetController netController;

    public Worker(int process_id, int total_process, String host_name, int base_port, int ld, int rebuild) {
        processId = process_id;
        totalProcess = total_process;
        hostName = host_name;
        basePort = base_port;
        leader = ld;
        reBuild = rebuild;

        currentCommand = "";

        voteStats = new Boolean[totalProcess+1];
        Arrays.fill(voteStats, false);
        voteAgreeNum = 0;
        ackStats = new Boolean[totalProcess+1];
        Arrays.fill(ackStats, false);
        ackNum = 0;

        playlist = new HashMap<String, String>();
        try {
            DTLog = new File("log_" + process_id + ".txt");
            if (!DTLog.exists()) {
                DTLog.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        processAlive = new Boolean[totalProcess+1];
        Arrays.fill(processAlive, true);
        aliveProcessNum = totalProcess;

        buildSocket();
        getReceivedMsgs(netController);
    }

    public void processMessage(String message) {
        String[] splits = message.split(" ");

        int sender_id = Integer.parseInt(splits[0]);
        switch (splits[1]) {
            case "vr":
                currentCommand = message;
                switch (splits[2]) {
                    case "add":
                        voteAdd(sender_id, splits[3], splits[4]);
                        break;
                    case "e":
                        voteEdit(sender_id, splits[3], splits[4], splits[5]);
                        break;
                    case "rm":
                        voteRm(sender_id, splits[3]);
                        break;
                    default:
                        System.err.println(String.format("[PARTICIPANT#%d] receives wrong command", processId));
                }
                break;
            case "v":
                countVote(sender_id, splits[2]);
                break;
            case "abt":
                break;
            case "rc":
                sendAcknowledge();
                break;
            case "ack":
                countAck(sender_id);
                break;
            case "c":
                performCommit();
                break;
            case "add":
                currentCommand = message;
                voteAdd(sender_id, splits[2], splits[3]);
                break;
            case "e":
                currentCommand = message;
                voteEdit(sender_id, splits[2], splits[3], splits[4]);
                break;
            case "rm":
                currentCommand = message;
                voteRm(sender_id, splits[2]);
                break;
            default:
                terminalLog("cannot recognize this command: " + splits[0]);
                break;
        }
    }

    /**
     * Count the number of received VOTE
     * @param sender_id
     * @param vote
     */
    public void countVote(int sender_id, String vote) {
        if (vote == "no") {
            Arrays.fill(voteStats, false);
            broadcastMsgs("abt");
        }
        else {
            if (!voteStats[sender_id]) {
                voteAgreeNum++;
                if (voteAgreeNum == aliveProcessNum-1) {
                    broadcastMsgs("rc");
                }
            }
        }
    }

    /**
     * Count the number of received ACK
     * @param sender_id
     */
    public void countAck(int sender_id) {
        if (!ackStats[sender_id]) {
            ackNum++;
            if (ackNum == aliveProcessNum-1) {
                broadcastMsgs("c");
                currentCommand = "# " + currentCommand; // format the command
                performCommit();
            }
        }
    }

    public void performCommit() {
        if (currentCommand == "") return;
        String[] splits = currentCommand.split(" ");

        switch (splits[2]) {
            case "add":
                add(splits[3], splits[4]);
                break;
            case "e":
                edit(splits[3], splits[4], splits[5]);
                break;
            case "rm":
                remove(splits[3]);
                break;
            default:
                break;
        }

        currentCommand = "";
        voteAgreeNum = ackNum = 0;
        Arrays.fill(voteStats, false);
        Arrays.fill(ackStats, false);
    }

    /**
     * Respond to VOTE_REQ
     * @param sender_id
     * @param songName
     * @param URL
     */
    public void voteAdd(int sender_id, String songName, String URL) {
        if (sender_id > 0) {
            if (playlist.containsKey(songName)) {
                netController.sendMsg(leader, String.format("%d v no", processId));
            } else {
                netController.sendMsg(leader, String.format("%d v yes", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                netController.sendMsg(0, "Request refused");
            } else {
                broadcastMsgs("vr add "+songName+" "+URL);
            }
        }
    }

    /**
     * Add song to playlist
     * @param songName
     * @param URL
     */
    public void add(String songName, String URL) {
        playlist.put(songName, URL);
        terminalLog("add <"+songName+","+URL+">");
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     */
    public void voteRm(int sender_id, String songName) {
        if (sender_id > 0) {
            if (playlist.containsKey(songName)) {
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                broadcastMsgs("vr rm "+songName);
            } else {
                netController.sendMsg(0, "Request refused");
            }
        }
    }

    /**
     * Remove song from playlist
     * @param songName
     */
    public void remove(String songName) {
        playlist.remove(songName);
        terminalLog("remove <"+songName+">");
    }

    /**
     * Respond to VOTE_REQ
     * @param sender_id
     * @param songName
     * @param URL
     */
    public void voteEdit(int sender_id, String songName, String newSongName, String URL) {
        if (sender_id > 0) {
            if (playlist.containsKey(songName)) {
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                broadcastMsgs("vr e "+songName+" "+newSongName+" "+URL);
            } else {
                netController.sendMsg(0, "Request refused");
            }
        }
    }

    /**
     * Edit song in playlist
     * @param songName
     * @param newSongName
     * @param newSongURL
     */
    public void edit(String songName, String newSongName, String newSongURL) {
        remove(songName);
        add(newSongName, newSongURL);
        terminalLog("update <"+songName+"> to <"+newSongName+","+newSongURL+">");
    }

    public void sendAcknowledge() {
        unicastMsgs(leader, "ack");
    }

    public void buildSocket() {
        Config config = null;
        try {
            config = new Config(processId, totalProcess, hostName, basePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        netController = new NetController(config);
    }


    /**
     * Unicast to a partner
     * @param dest_id
     * @param instruction
     */
    private void unicastMsgs(int dest_id, String instruction) {
        netController.sendMsg(dest_id, String.format("%d %s", processId, instruction));
    }

    /**
     * Broadcast to available partners
     * @param instruction
     */
    private void broadcastMsgs(String instruction) {
        for (int i = 1; i <= totalProcess; i++)
            if (i != processId && processAlive[i]) {
                unicastMsgs(i, instruction);
                System.out.println(String.format("[%s#%d] asks #%d to respond to \"%s\"", processId==leader?"COORDINATOR":"PARTICIPANT", processId, i, instruction));
            }
    }

    /**
     * Receive messages
     * @param netController
     */
    private void getReceivedMsgs(final NetController netController) {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    List<String> receivedMsgs = new ArrayList<String>(netController.getReceivedMsgs());
                    for (int i = 0; i < receivedMsgs.size(); i++) {
                        terminalLog(String.format("receive \"%s\"", receivedMsgs.get(i)));
                        processMessage(receivedMsgs.get(i));
                    }
                }
            }
        }).start();
    }

    private void terminalLog(String message) {
        System.err.println(String.format("[%s#%d] %s", processId==leader?"COORDINATOR":"PARTICIPANT", processId, message));
    }

    public static void main(String args[]) {
        int processId = Integer.parseInt(args[0]);
        if (args.length < 5) {
            System.err.println("[process "+processId+"] wrong parameters");
            System.exit(-1);
        }
        int    totalProcess = Integer.parseInt(args[1]);
        String hostName = args[2];
        int    basePort = Integer.parseInt(args[3]);
        int    leader   = Integer.parseInt(args[4]);
        int    reBuild  = Integer.parseInt(args[5]);

        Worker w = new Worker(processId, totalProcess, hostName, basePort, leader, reBuild);
        System.out.println("[process "+processId+"] started");
    }


}
