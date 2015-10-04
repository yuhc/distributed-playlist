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

    private Boolean[] voteStats;
    private int voteAgreeNum;
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
        voteStats = new Boolean[totalProcess+1];
        Arrays.fill(voteStats, false);
        voteAgreeNum = 0;

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
                switch (splits[2]) {
                    case "add":
                    case "e":
                        voteAddEdit(sender_id, splits[3]);
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
                break;
            case "ack":
                break;
            case "c":
                break;
            case "add":
            case "e":
                voteAddEdit(sender_id, splits[2]);
                break;
            case "rm":
                voteRm(splits[2]);
                break;
            default:
                System.err.println("[PROCESS#"+processId+"] cannot recognize this command: " + splits[0]);
                break;
        }
    }

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
     * Respond to VOTE_REQ
     * @param songName
     */
    public void voteAddEdit(int sender_id, String songName) {
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
                broadcastMsgs("vr add "+songName);
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
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     */
    public void voteRm(String songName) {
        if (playlist.containsKey(songName)) {
            netController.sendMsg(leader, String.format("%d v yes", processId));
        }
        else {
            netController.sendMsg(leader, String.format("%d v no", processId));
        }
    }

    /**
     * Remove song from playlist
     * @param songName
     */
    public void remove(String songName) {
        playlist.remove(songName);
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


    private void broadcastMsgs(String instruction) {
        for (int i = 1; i <= totalProcess; i++)
            if (i != processId && processAlive[i]) {
                netController.sendMsg(i, processId+" "+instruction);
                System.out.println(String.format("[COORDINATOR#%d] asks #%d to reply \"%s\"", processId, i, instruction));
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
                        System.err.println(String.format("[PROCESS#%d] receive \"%s\"", processId, receivedMsgs.get(i)));
                        processMessage(receivedMsgs.get(i));
                    }
                }
            }
        }).start();
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
