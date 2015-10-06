package edu.utexas.cs.threepc;

import edu.utexas.cs.netutil.*;

import javax.swing.Timer;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
    private String currentState;

    private Boolean[] voteStats;
    private int voteNum;
    private Boolean[] ackStats;
    private int ackNum;
    private Boolean[] processAlive;
    private int aliveProcessNum;
    private Boolean[] hasRespond;
    private Boolean rejectNextChange;

    private String hostName;
    private String lastCommandInLog;
    private int    basePort;
    private int    reBuild;
    private NetController netController;

    public static final String PREFIX_COMMAND    = "COMMAND";

    /* COORDINATOR */
    public static final String STATE_WAITVOTE    = "WAITVOTE";
    public static final String STATE_PRECOMMITED = "PRECOMMITED";
    public static final String STATE_WAITACK     = "WAITACK";
    public static final String STATE_ACKED       = "ACKED";
    /* PARTICIPANT */
    public static final String STATE_VOTED       = "VOTED";
    public static final String STATE_PRECOMMIT   = "PRECOMMIT";
    /* BOTH */
    public static final String STATE_WAIT        = "WAIT";
    public static final String STATE_RECOVER     = "RECOVER";
    public static final String STATE_COMMIT      = "COMMIT";
    public static final String STATE_ABORT       = "ABORT";

    private Timer timer;
    public static final int    TIME_OUT        = 3000;
    private int   decision;

    public Worker(final int processId, final int totalProcess, String hostName, int basePort, int ld, int rebuild) {
        this.processId = processId;
        this.totalProcess = totalProcess;
        this.hostName = hostName;
        this.basePort = basePort;
        leader = ld;
        reBuild = rebuild;

        currentCommand = "";

        voteStats = new Boolean[this.totalProcess+1];
        Arrays.fill(voteStats, false);
        voteNum = 0;
        ackStats = new Boolean[this.totalProcess+1];
        Arrays.fill(ackStats, false);
        ackNum = 0;
        hasRespond = new Boolean[this.totalProcess+1];
        rejectNextChange = false;

        playlist = new HashMap<String, String>();
        try {
            File playlistInit = new File("../data/playlist_init_"+processId+".txt");
            if (playlistInit.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(playlistInit));
                String line;
                while((line = br.readLine()) != null) {
                    String[] splits = line.split("==>");
                    if (splits.length != 2) break;
                    splits[0] = splits[0].trim();
                    splits[1] = splits[1].trim();
                    playlist.put(splits[0], splits[1]);
                    terminalLog(String.format("<%s, %s> is imported", splits[0], splits[1]));
                }
                br.close();
            }
            DTLog = new File("../log/dt_" + processId + ".log");
            if (!DTLog.exists() || reBuild == 0) {
                DTLog.getParentFile().mkdirs();
                DTLog.createNewFile();
            } else {
                // Read recovery log and try to reconstruct the state
                currentState = STATE_RECOVER;
                BufferedReader br = new BufferedReader(new FileReader(DTLog));
                String line;
                while ((line = br.readLine()) != null) {
                    processRecovery(line);
                }
                // TODO: not finished for recovery
                // Need to process the last state if it is not commit or abort.
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
        currentState = STATE_WAIT;

        timer = new Timer(TIME_OUT, new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                switch (currentState) {
                    case STATE_WAITVOTE:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                                processAlive[i] = false;
                            }
                        performAbort();
                        break;
                    case STATE_WAITACK:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                                processAlive[i] = false;
                            }
                        logWrite(STATE_ACKED);
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && hasRespond[i])
                                unicastMsgs(i, "c");
                        currentCommand = "# " + currentCommand; // format the command
                        performCommit();
                        break;
                    default: // Termination protocol?
                        break;
                }
            }
        });
        timer.setRepeats(false);
    }

    public void processRecovery(String message) {
        if (message.startsWith(PREFIX_COMMAND)) {
            String[] splits = message.split(":");
            if (splits[1].equals("rnc"))
                rejectNextChange = true;
            else
                currentCommand = splits[1];
        }
        else if (!message.startsWith(STATE_RECOVER)){
            switch (message) {
                case STATE_ABORT:
                    currentCommand = "";
                    currentState = message;
                    break;
                case STATE_COMMIT:
                    performCommit();
                case "yes":
                    return;
                case "no":
                    currentCommand = "";
                    currentState = STATE_ABORT;
                    break;
                case STATE_PRECOMMITED:
                case STATE_PRECOMMIT:
                case STATE_ACKED:
                    currentState = message;
                    return;
                default:
                    terminalLog("unrecognized command in recovery log: " + message);
            }
        }
    }

    public void processMessage(String message) {
        String[] splits = message.split(" ");

        int senderId = Integer.parseInt(splits[0]);
        switch (splits[1]) {
            case "vr":
                currentCommand = message;
                logWrite(PREFIX_COMMAND+":"+message);
                currentState = STATE_VOTED;
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
                        terminalLog("receives wrong command");
                }
                break;
            case "v":
                countVote(senderId, splits[2]);
                break;
            case "abt":
                performAbort();
                break;
            case "pc": // PRECOMMIT
                logWrite(STATE_PRECOMMIT);
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
            case "rnc":
                rejectNextChange = true;
                logWrite(PREFIX_COMMAND+":rnc");
                break;
            case "rm":
                currentCommand = message;
                voteRm(senderId, splits[2]);
                break;
            case "pl":
            	printPlayList();
            	break;
            default:
                terminalLog("cannot recognize this command: " + splits[0]);
                break;
        }
    }

    /**
     * Print local playlist to storage
     */
    private void printPlayList() {
        if (processId == leader)
            broadcastMsgs("pl");

        try {
            File playlistFile = new File(String.format("../log/playlist_%d.txt", processId));
            playlistFile.getParentFile().mkdirs();
            playlistFile.createNewFile();

            BufferedWriter out = new BufferedWriter(new FileWriter(String.format("../log/playlist_%d.txt", processId)));

            Iterator<Map.Entry<String, String>> it = playlist.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> pairs = it.next();
                out.write(pairs.getKey() + "\t==>\t" + pairs.getValue() + "\n");
            }

            out.close();
            terminalLog("outputs local playlist");
        } catch (IOException e) {
            e.printStackTrace();
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
     * Wait participants to vote
     */
    public void waitVote() {
        logWrite(PREFIX_COMMAND+":# "+currentCommand);
        timer.start();
        voteNum = 0;
        decision = 1;
        currentState = STATE_WAITVOTE;
        Arrays.fill(hasRespond, false);
    }

    /**
     * Count the number of received VOTE
     * @param senderId
     * @param vote
     */
    public void countVote(int senderId, String vote) {
        voteNum++;
        hasRespond[senderId] = true;
        if (vote.equals("no")) {
            decision = 0;
            if (voteNum == aliveProcessNum-1 && decision == 0) {
                timer.stop();
                performAbort();
            }
        }
        else {
            if (!voteStats[senderId]) {
                voteStats[senderId] = true;
                if (voteNum == aliveProcessNum-1 && decision == 1) {
                    timer.stop();
                    logWrite(STATE_PRECOMMITED);
                    broadcastMsgs("pc");
                    waitAck();
                }
            }
        }
    }

    /**
     * Wait participants to reply ACK
     */
    public void waitAck() {
        logWrite(STATE_PRECOMMITED);
        timer.start();
        ackNum = 0;
        currentState = STATE_WAITACK;
        Arrays.fill(hasRespond, false);
    }

    /**
     * Count the number of received ACK
     * @param senderId
     */
    public void countAck(int senderId) {
        ackNum++;
        hasRespond[senderId] = true;
        if (!ackStats[senderId]) {
            if (ackNum == aliveProcessNum-1) {
                timer.stop();
                logWrite(STATE_ACKED);
                broadcastMsgs("c");
                currentCommand = "# " + currentCommand; // format the command
                performCommit();
            }
        }
    }

    /**
     * Perform COMMIT operation
     */
    public void performCommit() {
        if (currentCommand.equals("")) return;
        if (currentState.equals(STATE_RECOVER))
            logWrite(STATE_RECOVER+":"+currentCommand);
        else
            logWrite(STATE_COMMIT);

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
        voteNum = ackNum = 0;
        Arrays.fill(voteStats, false);
        Arrays.fill(ackStats, false);
    }

    /**
     * Perform ABORT operation
     */
    public void performAbort() {
        if (currentState.equals(STATE_ABORT))
            return;
        timer.stop();
        if (currentState.equals(STATE_WAIT) || currentState.equals(STATE_VOTED)) {
            terminalLog("aborts the request");
        }
        else {
            if (currentState.equals(STATE_WAITVOTE)) {
                for (int i = 1; i <= totalProcess; i++)
                    if (i != processId && processAlive[i] && voteStats[i])
                        unicastMsgs(i, "abt");
            }
            else {
                broadcastMsgs("abt");
            }
            terminalLog("aborts the request");
        }
        logWrite(STATE_ABORT);
        if (leader == processId)
            currentState = STATE_WAIT;
        else
            currentState = STATE_ABORT;

        currentCommand = "";
        voteNum = ackNum = 0;
        Arrays.fill(voteStats, false);
        Arrays.fill(ackStats, false);
    }


    private String aliveProcessList() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < processAlive.length; i++) {
            if (processAlive[i]) {
                sb.append(i);
                sb.append(":");
            }
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }

    /**
     * Respond to VOTE_REQ
     * @param senderId
     * @param songName
     * @param URL
     */
    public void voteAdd(int senderId, String songName, String URL) {
        if (senderId > 0) {
            if (playlist.containsKey(songName) || rejectNextChange) {
                performAbort();
                netController.sendMsg(leader, String.format("%d v no", processId));
            } else {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            }
        }
        else {
            if (playlist.containsKey(songName) || rejectNextChange) {
                performAbort();
            } else {
                broadcastMsgs("vr add "+songName+" "+URL+" "+aliveProcessList());
                waitVote();
            }
        }
        rejectNextChange = false;
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
            if (playlist.containsKey(songName) && !rejectNextChange) {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                performAbort();
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName) && !rejectNextChange) {
                broadcastMsgs("vr rm "+songName+" "+aliveProcessList());
                waitVote();
            } else {
                performAbort();
            }
        }
        rejectNextChange = false;
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
            if (playlist.containsKey(songName) && !playlist.containsKey(newSongName) && !rejectNextChange) {
                logWrite("yes");
                netController.sendMsg(leader, String.format("%d v yes", processId));
            } else {
                performAbort();
                netController.sendMsg(leader, String.format("%d v no", processId));
            }
        }
        else {
            if (playlist.containsKey(songName) && !playlist.containsKey(newSongName) && !rejectNextChange) {
                broadcastMsgs("vr e "+songName+" "+newSongName+" "+URL+" "+aliveProcessList());
                waitVote();
            } else {
                performAbort();
            }
        }
        rejectNextChange = false;
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
        terminalLog("update <"+songName+"> to <"+newSongName+", "+newSongURL+">");
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
