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

    public Worker(int processId, int totalProcess, String hostName, int basePort, int ld, int rebuild) {
        this.processId = processId;
        this.totalProcess = totalProcess;
        this.hostName = hostName;
        this.basePort = basePort;
        leader = ld;
        reBuild = rebuild;

        currentCommand = "";

        voteStats = new Boolean[this.totalProcess +1];
        Arrays.fill(voteStats, false);
        voteAgreeNum = 0;
        ackStats = new Boolean[this.totalProcess +1];
        Arrays.fill(ackStats, false);
        ackNum = 0;

        playlist = new HashMap<String, String>();
        try {
            DTLog = new File("log_" + processId + ".txt");
            if (!DTLog.exists()) {
                DTLog.createNewFile();
            }
            File playlistInit = new File("data/playlist_init_"+processId+".txt");
            if (playlistInit.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(playlistInit));
                String line = null;
                while((line = br.readLine()) != null) {
                    String[] splits = line.split(",");
                    playlist.put(splits[0], splits[1]);
                }
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        processAlive = new Boolean[this.totalProcess +1];
        Arrays.fill(processAlive, true);
        aliveProcessNum = this.totalProcess;

        buildSocket();
        getReceivedMsgs(netController);
    }

    public void processMessage(String message) {
        String[] splits = message.split(" ");

        int senderId = Integer.parseInt(splits[0]);
        switch (splits[1]) {
            case "vr":
                currentCommand = message;
                switch (splits[2]) {
                    case "add":
                        voteAdd(senderId, splits[3], splits[4]);
                        break;
                    case "e":
                         voteEdit(senderId, splits[3], splits[4], splits[5]);
                        break;
                    case "rm":
                        voteRm(senderId, splits[3]);
                        break;
                    default:
                        System.err.println(String.format("[PARTICIPANT#%d] receives wrong command", processId));
                }
                break;
            case "v":
                countVote(senderId, splits[2]);
                break;
            case "abt":
                logWrite("abt");
                break;
            case "rc":
                sendAcknowledge();
                break;
            case "ack":
                countAck(senderId);
                break;
            case "c":
                performCommit();
                break;
            case "add":
                currentCommand = message;
                voteAdd(senderId, splits[2], splits[3]);
                break;
            case "e":
                currentCommand = message;
                voteEdit(senderId, splits[2], splits[3], splits[4]);
                break;
            case "rm":
                currentCommand = message;
                voteRm(senderId, splits[2]);
                break;
            default:
                terminalLog("cannot recognize this command: " + splits[0]);
                break;
        }
    }

    private void logWrite(String str) {
        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(DTLog, true)))) {
            pw.println(str);
            pw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Count the number of received VOTE
     * @param senderId
     * @param vote
     */
    public void countVote(int senderId, String vote) {
        if (vote == "no") {
            Arrays.fill(voteStats, false);
            logWrite("abt");
            broadcastMsgs("abt");
        }
        else {
            if (!voteStats[senderId]) {
                voteAgreeNum++;
                if (voteAgreeNum == aliveProcessNum-1) {
                    logWrite("rc");
                    broadcastMsgs("rc");
                }
            }
        }
    }

    /**
     * Count the number of received ACK
     * @param senderId
     */
    public void countAck(int senderId) {
        if (!ackStats[senderId]) {
            ackNum++;
            if (ackNum == aliveProcessNum-1) {
                logWrite("c");
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
     * @param senderId
     * @param songName
     * @param URL
     */
    public void voteAdd(int senderId, String songName, String URL) {
        if (senderId > 0) {
            if (playlist.containsKey(songName)) {
                logWrite("abt");
                netController.sendMsg(leader, String.format("%d v no", processId));
            } else {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                netController.sendMsg(0, "Request refused");
            } else {
                logWrite("start_3pc:add "+songName+" "+URL);
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
    public void voteRm(int senderId, String songName) {
        if (senderId > 0) {
            if (playlist.containsKey(songName)) {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                logWrite("abt");
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                logWrite("start_3pc:rm "+songName);
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
     * @param senderId
     * @param songName
     * @param URL
     */
    public void voteEdit(int senderId, String songName, String newSongName, String URL) {
        if (senderId > 0) {
            if (playlist.containsKey(songName)) {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                logWrite("abt");
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName)) {
                logWrite("start_3pc:e "+songName+" "+newSongName+" "+URL);
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
     * @param destId
     * @param instruction
     */
    private void unicastMsgs(int destId, String instruction) {
        netController.sendMsg(destId, String.format("%d %s", processId, instruction));
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
        System.out.println(String.format("[%s#%d] %s", processId == leader ? "COORDINATOR" : "PARTICIPANT", processId, message));
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
