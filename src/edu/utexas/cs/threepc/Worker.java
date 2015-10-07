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
    private int       voteNum;
    private Boolean[] ackStats;
    private int       ackNum;
    private Boolean[] processAlive;
    private int       aliveProcessNum;
    private Boolean[] hasRespond;
    private Boolean   rejectNextChange;
    private Boolean[] stateReqAck;
    private int       stateReqAckNum;

    private String hostName;
    private String lastCommandInLog;
    private int    basePort;
    private int    reBuild;
    private NetController netController;

    public static final String PREFIX_COMMAND    = "COMMAND:";
    public static final String PREFIX_ALIVEPROC  = "ALIVEPROC:";
    public static final String PREFIX_PROCNUM    = "PROCNUM:";
    public static final String PREFIX_VOTE       = "VOTE:";

    /* COORDINATOR */
    public static final String STATE_START       = "START-3PC";
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

    /**
     *
     * @param processId
     * @param totalProcess  equals to 0 if created as participant or recovered process
     * @param hostName
     * @param basePort
     * @param ld
     * @param rebuild
     */
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
        Arrays.fill(hasRespond, false);
        rejectNextChange = false;
        stateReqAckNum = 0;
        stateReqAck = new Boolean[this.totalProcess+1];
        Arrays.fill(stateReqAck, false);

        currentState = STATE_WAIT;

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
                DTLog.delete();
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
                if (!currentState.equals(STATE_ABORT) || !currentState.equals(STATE_COMMIT)) {
                    // TODO: How do I know whether I have sent "yes" out
                    // If p writes yes to log but crash before send it, c will time out on receiving vote on p and c will decide abort
                    // If p writes yes to log sends the vote but crash immediately, c will time out on getting ack from p
                    // c will decide commit....How to handle this problem? 
                }
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        buildSocket();
        getReceivedMsgs(netController);

        timer = new Timer(TIME_OUT, new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                switch (currentState) {
                    case STATE_WAITVOTE:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                                processAlive[i] = false;
                                aliveProcessNum--;
                            }
                        performAbort();
                        break;
                    case STATE_WAITACK:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                                processAlive[i] = false;
                                aliveProcessNum--;
                            }
                        logWrite(STATE_ACKED);
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && processAlive[i] && hasRespond[i])
                                unicastMsgs(i, "c");
                        currentCommand = "# " + currentCommand; // format the command
                        performCommit();
                        break;
                    default: // TODO: Termination protocol here?
                        break;
                }
            }
        });
        timer.setRepeats(false);

        if (totalProcess > 0) {
            logWrite(PREFIX_PROCNUM+totalProcess);
            processAlive = new Boolean[this.totalProcess + 1];
            Arrays.fill(processAlive, true);
            aliveProcessNum = this.totalProcess;
        }

        // Send STATE_REQ
        if (rebuild == 0 && processId != leader || rebuild == 1) {
            if (rebuild == 0) {
                unicastMsgs(leader, "sr");
            }
            else {
                Arrays.fill(processAlive, false);
                broadcastMsgs("sr");
                timer.start();
            }
        }
    }

    public void processRecovery(String message) {
        String[] splits = message.split(":");

        if (message.startsWith(PREFIX_COMMAND)) {
            if (splits[1].equals("rnc"))
                rejectNextChange = true;
            else
                currentCommand = splits[1];
        }
        else if (message.startsWith(PREFIX_PROCNUM)) {
            totalProcess = Integer.parseInt(splits[1]);
            terminalLog("total number of processes is "+totalProcess);
        }
        else if (!message.startsWith(STATE_RECOVER)){
            // TODO: I think recovery is not fully implemented.
            switch (message) {
                case STATE_START:
                    leader = processId;
                    break;
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
                logWrite(PREFIX_COMMAND+message);
                currentState = STATE_VOTED;
                String aliveProcessList = splits[splits.length-1];
                if (totalProcess == 0) {
                    String[] aliveProcesses = aliveProcessList.split(",");
                    totalProcess = Integer.parseInt(aliveProcesses[aliveProcesses.length-1]);
                    logWrite(PREFIX_PROCNUM+totalProcess);
                }
                leader = senderId;
                switch (splits[2]) {
                    case "add":
                        updateProcessList(aliveProcessList);
                        voteAddParticipant(splits[3]);
                        break;
                    case "e":
                        updateProcessList(aliveProcessList);
                        voteEditParticipant(splits[3], splits[4]);
                        break;
                    case "rm":
                        updateProcessList(aliveProcessList);
                        voteRmParticipant(splits[3]);
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
                voteAddCoordinator(splits[2], splits[3]);
                break;
            case "e":
                currentCommand = message;
                voteEditCoordinator(splits[2], splits[3], splits[4]);
                break;
            case "rnc":
                rejectNextChange = true;
                logWrite(PREFIX_COMMAND+"rnc");
                break;
            case "rm":
                currentCommand = message;
                voteRmCoordinator(splits[2]);
                break;
            case "pl":
            	printPlayList();
            	break;
            case "sr": // STATE_REQ
                unicastMsgs(senderId, "sa "+totalProcess+" "+currentState+" "+aliveProcessList());
                break;
            case "sa": // STATE_ACK
                int totalProcessNum = Integer.parseInt(splits[2]);
                countStateAck(senderId, totalProcessNum, splits[3], splits[4]);
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

    public void countStateAck(int senderId, int totalProcessNum, String state, String aliveList) {
        if (reBuild == 0) { // initial
            updateProcessList(aliveList);
            logWrite(PREFIX_PROCNUM+totalProcessNum);
            totalProcess = totalProcessNum;
            processAlive = new Boolean[this.totalProcess + 1];
            Arrays.fill(processAlive, true);
            aliveProcessNum = this.totalProcess;
        }
        else {              // ask for help or recover
            stateReqAckNum++;
            processAlive[senderId] = true;

        }
    }

    /**
     * Wait participants to vote
     */
    public void waitVote() {
        logWrite(PREFIX_COMMAND+"# "+currentCommand);
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
        if (!processAlive[senderId]) {
            aliveProcessNum++;
            processAlive[senderId] = true;
        }
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
        if (!processAlive[senderId]) {
            aliveProcessNum++;
            processAlive[senderId] = true;
        }
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
        if (currentState.equals(STATE_START) || currentState.equals(STATE_WAIT) || currentState.equals(STATE_VOTED)) {
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
        currentState = STATE_ABORT;

        currentCommand = "";
        voteNum = ackNum = 0;
        Arrays.fill(voteStats, false);
        Arrays.fill(ackStats, false);
    }


    /**
     * Build alive process list
     * @return
     */
    private String aliveProcessList() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= totalProcess; i++) {
            if (processAlive[i]) {
                sb.append(i);
                sb.append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Update alive process list using "1,2,3"
     * @param newList
     */
    public void updateProcessList(String newList) {
        String[] newSplits = newList.split(",");
        Arrays.fill(processAlive, false);
        for (int i = 0; i < newSplits.length; i++)
            processAlive[Integer.parseInt(newSplits[i])] = true;
        logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    public void setProcessAlive(String processStr) {
        int processId = Integer.parseInt(processStr);
        processAlive[processId] = true;
        logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    public void setProcessAlive(int processId) {
        processAlive[processId] = true;
        logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    public void setProcessDead(String processStr) {
        int processId = Integer.parseInt(processStr);
        processAlive[processId] = false;
        logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    public void setProcessDead(int processId) {
        processAlive[processId] = false;
        logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     * @param URL
     */
    public void voteAddCoordinator(String songName, String URL) {
        currentState = STATE_START;
        logWrite(STATE_START);
        if (playlist.containsKey(songName) || rejectNextChange) {
            logWrite(PREFIX_COMMAND+"# "+currentCommand);
            performAbort();
        } else {
            currentState = STATE_WAITVOTE;
            broadcastMsgs("vr add "+songName+" "+URL+" "+aliveProcessList());
            waitVote();
        }
        rejectNextChange = false;
    }

    public void voteAddParticipant(String songName) {
        if (playlist.containsKey(songName) || rejectNextChange) {
            performAbort();
            netController.sendMsg(leader, String.format("%d v no", processId));
        } else {
            logWrite(PREFIX_VOTE+"YES");
            netController.sendMsg(leader, String.format("%d v yes", processId));
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
        terminalLog("add <" + songName + "," + URL + ">");
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     */
    public void voteRmCoordinator(String songName) {
        currentState = STATE_START;
        logWrite(STATE_START);
        if (playlist.containsKey(songName) && !rejectNextChange) {
            currentState = STATE_WAITVOTE;
            broadcastMsgs("vr rm "+songName+" "+aliveProcessList());
            waitVote();
        } else {
            logWrite(PREFIX_COMMAND+"# "+currentCommand);
            performAbort();
        }
        rejectNextChange = false;
    }

    public void voteRmParticipant(String songName) {
        if (playlist.containsKey(songName) && !rejectNextChange) {
            logWrite(PREFIX_VOTE+"YES");
            netController.sendMsg(leader, String.format("%d v yes", processId));
        } else {
            performAbort();
            netController.sendMsg(leader, String.format("%d v no", processId));
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
     * @param songName
     * @param URL
     */
    public void voteEditCoordinator(String songName, String newSongName, String URL) {
        currentState = STATE_START;
        logWrite(STATE_START);
        if (playlist.containsKey(songName) && !playlist.containsKey(newSongName) && !rejectNextChange) {
            currentState = STATE_WAITVOTE;
            broadcastMsgs("vr e "+songName+" "+newSongName+" "+URL+" "+aliveProcessList());
            waitVote();
        } else {
            logWrite(PREFIX_COMMAND+"# "+currentCommand);
            performAbort();
        }
        rejectNextChange = false;
    }

    public void voteEditParticipant(String songName, String newSongName) {
        if (playlist.containsKey(songName) && !playlist.containsKey(newSongName) && !rejectNextChange) {
            logWrite(PREFIX_VOTE+"YES");
            netController.sendMsg(leader, String.format("%d v yes", processId));
        } else {
            performAbort();
            netController.sendMsg(leader, String.format("%d v no", processId));
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
