package edu.utexas.cs.threepc;

import edu.utexas.cs.netutil.*;

import javax.swing.Timer;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Note: maybe use sock.connect to set a timeout.
 */
public class Worker {

    private int  processId;
    private int  totalProcess;
    private int  leader, oldLeader;
    private File DTLog;
    private Map<String, String> playlist;

    private String currentCommand;
    private String currentState;
    private boolean isRecover;

    private int       viewNumber;
    private Boolean[] processAlive;

    private Boolean[] voteStats;
    private int       voteNum;
    private Boolean[] ackStats;
    private int       ackNum;
    private Boolean[] hasRespond;
    private Boolean   rejectNextChange;
    private Boolean[] stateReqAck;
    private String[]  stateReqList;
    private int       stateReqAckNum;

    private String hostName;
    private int    basePort;
    private int    reBuild;
    private NetController netController;
    private int msgCounter;
    private int msgAllowed;
    private boolean countingMsg;
    private boolean canSendMsg;
    private int instanceNum;
    private String lastLine;

    public static final String PREFIX_COMMAND    = "COMMAND:";
    public static final String PREFIX_PROCNUM    = "PROCNUM:";
    public static final String PREFIX_VOTE       = "VOTE:";
    public static final String PREFIX_START      = "START-3PC:";

    /* COORDINATOR */
    public static final String STATE_START       = "START-3PC";
    public static final String STATE_WAITVOTE    = "WAITVOTE";
    public static final String STATE_PRECOMMITED = "PRECOMMITED";
    public static final String STATE_WAITACK     = "WAITACK";
    public static final String STATE_ACKED       = "ACKED";
    /* PARTICIPANT */
    public static final String STATE_VOTEREQ     = "VOTEREQ";
    public static final String STATE_VOTED       = "VOTED";
    public static final String STATE_PRECOMMIT   = "PRECOMMIT";
    public static final String STATE_STREQ       = "STATE_REQ";
    public static final String STATE_SELETC      = "SELECT_COORDINATOR";
    /* BOTH */
    public static final String STATE_WAIT        = "WAIT";
    public static final String STATE_RECOVER     = "RECOVER";
    public static final String STATE_COMMIT      = "COMMIT";
    public static final String STATE_ABORT       = "ABORT";
    public static final String STATE_NOTANS      = "NOTANS";

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
        this.countingMsg = false;
        this.msgCounter = 0;
        this.instanceNum = 0;
        this.canSendMsg = true;
        leader = ld;
        oldLeader = -1;
        reBuild = rebuild;

        currentCommand = "";
        rejectNextChange = false;
        currentState = STATE_WAIT;

        playlist = new HashMap<String, String>();
        isRecover = true;
        try {
            DTLog = new File("../log/dt_" + processId + ".log");
            if (!DTLog.exists() || reBuild == 0) {
                DTLog.getParentFile().mkdirs();
                DTLog.delete();
                DTLog.createNewFile();
            } else {
                // Read recovery log and try to reconstruct the state
                terminalLog("starts recovering");
                currentState = STATE_RECOVER;
                BufferedReader br = new BufferedReader(new FileReader(DTLog));
                String line;
                lastLine = "";
                while ((line = br.readLine()) != null) {
                    processRecovery(line);
                }

                // TODO: not finished for recovery
                // Need to process the last state if it is not commit or abort.
                if (!currentCommand.equals("") && (!currentState.equals(STATE_ABORT) || !currentState.equals(STATE_COMMIT))) {
                    // p fails before sending yes
                    terminalLog(currentCommand + " # " + currentState + " # " + lastLine);
                    if (currentCommand.split(" ")[2].equals("vr") && currentState.equals(STATE_VOTEREQ)) {
                        performAbort();
                    } else if (currentState.startsWith(STATE_VOTED) ||
                            currentState.equals(STATE_PRECOMMIT)) {
                        // TODO: ask other process for help
                        broadcastMsgs("rr "+instanceNum);
                    }
                }
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        isRecover = false;

        if (rebuild == 0) {
            buildSocket();
            getReceivedMsgs(netController);
        }

        timer = new Timer(TIME_OUT, new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                switch (currentState) {
                    // Coordinator waits for VOTE
                    case STATE_WAITVOTE:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                            }
                        performAbort();
                        break;
                    // Coordinator waits for ACK
                    case STATE_WAITACK:
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                            }
                        logWrite(STATE_ACKED);
                        for (int i = 1; i <= totalProcess; i++)
                            if (i != processId && hasRespond[i])
                                unicastMsgs(i, "c");
                        currentCommand = "# " + currentCommand; // format the command
                        performCommit();
                        break;
                    // Participant waits for PRECOMMIT
                    case STATE_VOTED:
                        stateReqAckNum = 0;
                        Arrays.fill(hasRespond, false);
                        Arrays.fill(stateReqList, STATE_NOTANS);
                        broadcastMsgs("sr");
                        timer.start();
                        break;
                    // Participant waits for STATE_REQ ACK
                    case STATE_STREQ:
                        for (int i = 1; i < totalProcess; i++)
                            if (i != processId && !hasRespond[i]) {
                                terminalLog(String.format("participant %d times out", i));
                            }
                        if (currentState.equals(STATE_VOTED)) {
                            performAbort();
                            broadcastMsgs("abt");
                        }
                        break;
                    // Wait STATE_REQ from new coordinator
                    case STATE_SELETC:
                        processAlive[viewNumber] = false;
                        electCoordinator();
                        break;
                    default: // TODO: Termination protocol here?
                        break;
                }
            }
        });
        timer.setRepeats(false);

        if (totalProcess > 0 && processId == leader && reBuild == 0) {
            logWrite(PREFIX_PROCNUM+totalProcess);
            initializeArrays();
        }

        // Send STATE_REQ
        if (rebuild == 0 && processId != leader) {
            unicastMsgs(leader, "ir");
        }
//        else if (rebuild == 1) {
//            stateReqAckNum = 0;
//            Arrays.fill(hasRespond, false);
//            Arrays.fill(stateReqList, STATE_NOTANS);
//            broadcastMsgs("sr");
//            timer.start();
//        }
    }

    private void initializeArrays() {
        processAlive = new Boolean[totalProcess+1];
        Arrays.fill(processAlive, true);
        voteStats = new Boolean[totalProcess+1];
        Arrays.fill(voteStats, false);
        voteNum = 0;
        ackStats = new Boolean[totalProcess+1];
        Arrays.fill(ackStats, false);
        ackNum = 0;
        hasRespond = new Boolean[totalProcess+1];
        Arrays.fill(hasRespond, false);
        stateReqAckNum = 0;
        stateReqAck = new Boolean[totalProcess+1];
        Arrays.fill(stateReqAck, false);
        stateReqList = new String[totalProcess+1];
        Arrays.fill(stateReqList, STATE_NOTANS);
    }

    public void processRecovery(String message) {
        if (!message.startsWith(STATE_RECOVER))
            lastLine = message;

        String[] splits = message.split(":");

        if (message.startsWith(PREFIX_COMMAND)) {
            if (splits[1].equals("rnc"))
                rejectNextChange = true;
            else {
                currentCommand = splits[1];
                instanceNum = Integer.parseInt(splits[1].split(" ")[1]);
            }
        }
        else if (message.startsWith(PREFIX_PROCNUM)) {
            totalProcess = Integer.parseInt(splits[1]);
            terminalLog("total number of processes is "+totalProcess);
            initializeArrays();
            buildSocket();
            getReceivedMsgs(netController);
        }
        else if (message.startsWith(PREFIX_VOTE)) {
            if (splits[1].equals("YES")) {
                currentState = STATE_VOTED;
            }
        }
        else if (message.startsWith(PREFIX_START)) {
            leader = processId;
            instanceNum = Integer.parseInt(splits[0]);
        }
        else if (!message.startsWith(STATE_RECOVER)){
            // TODO: I think recovery is not fully implemented.
            switch (message) {
                case STATE_ABORT:
                    currentCommand = "";
                    currentState = message;
                    break;
                case STATE_VOTEREQ:
                    currentState = STATE_VOTEREQ;
                    break;
                case STATE_COMMIT:
                    performCommit();
                    currentCommand = "";
                    currentState = message;
                    break;
                case STATE_PRECOMMITED:
                case STATE_PRECOMMIT:
                case STATE_ACKED:
                    currentState = message;
                    break;
                default:
                    terminalLog("unrecognized command in recovery log: " + message);
            }
        }
    }

    public void processMessage(String message) {
        String[] splits = message.split(" ");

        int senderId = Integer.parseInt(splits[0]);
        if (splits.length >= 3 && splits[2].equals("vr")) {
            currentCommand = message;
            logWrite(PREFIX_COMMAND+message);
            logWrite(STATE_VOTEREQ);
            currentState = STATE_VOTED;

            String processAliveList = splits[splits.length-1];
            instanceNum = Integer.parseInt(splits[1]);
            leader = senderId;
            switch (splits[3]) {
                case "add":
                    voteAddParticipant(splits[4]);
                    break;
                case "e":
                    voteEditParticipant(splits[4], splits[5]);
                    break;
                case "rm":
                    voteRmParticipant(splits[4]);
                    break;
                default:
                    terminalLog("receives wrong command");
            }
        }
        else
        switch (splits[1]) {
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
            case "ir": // INITIAL_REQUEST
                unicastMsgs(senderId, "ia "+totalProcess);
                break;
            case "ia": // INITIAL_REQ_ACK
                int totalProcessNum = Integer.parseInt(splits[2]);
                initialRequest(totalProcessNum);
                break;
            case "ue": // UR_ELECTED
                if (leader != processId) oldLeader = leader;
                leader = processId;
                termination();
                break;
            case "sr": // STATE_REQ
                if (senderId == oldLeader) break;
                timer.stop();
                for (int i = 1; i < senderId; i++)
                    processAlive[i] = false;
                oldLeader = leader;
                leader = senderId;
                unicastMsgs(senderId, "sa "+currentState);
                break;
            case "sa": // STATE_ACK
                countStateAck(senderId, splits[2]);
                break;
            case "rr": // RECOVER_REQ
                // TODO: timeout
                timer.stop();
                int reqInstanceNum = Integer.parseInt(splits[2]);
                unicastMsgs(senderId, "ra "+readLog(reqInstanceNum));
                break;
            case "ra": // RECOVER_REQ_ANS
                // TODO: filter received messages
                break;
            case "pm": // partial message
                msgCounter = 0;
                countingMsg = true;
                msgAllowed = Integer.parseInt(splits[2]);
                break;
            default:
                terminalLog("cannot recognize this command: " + message);
                break;
        }
    }

    private String readLog(int reqInstanceNum) {
        DTLog = new File("../log/dt_" + processId + ".log");
        // Read recovery log and try to reconstruct the state
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(DTLog));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        int logInstanceNum;
        try {
            while (true) {
                line = br.readLine();
                if (line.startsWith(PREFIX_START) || line.startsWith(PREFIX_COMMAND)) {
                    if (line.startsWith(PREFIX_START))
                        logInstanceNum = Integer.parseInt(line.split(":")[1]);
                    else
                        logInstanceNum = Integer.parseInt(line.split(" ")[1]);
                    if (logInstanceNum == reqInstanceNum)
                        break;
                }
            }

            lastLine = "";
            while ((line = br.readLine()) != null) {
                if (line.equals(STATE_ABORT) || line.equals(STATE_COMMIT))
                    return line;
                lastLine = line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastLine;
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
    
    public void initialRequest(int totalProcessNum) {
        logWrite(PREFIX_PROCNUM + totalProcessNum);
        totalProcess = totalProcessNum;
        initializeArrays();
    }

    /**
     * Termination Protocol
     * Deal with ACK of STATE_REQ
     * @param senderId
     * @param state
     */
    public void countStateAck(int senderId, String state) {
        stateReqAckNum++;
        hasRespond[senderId] = true;
        stateReqList[senderId] = state;
        if (stateReqAckNum == totalProcess - 1) {
            timer.stop();
            // TR1
            for (int i = 1; i <= totalProcess; i++)
                if (stateReqList[i] == STATE_ABORT) {
                    performAbort();
                    for (int j = 1; j <= totalProcess; j++)
                        if (stateReqList[j] != STATE_ABORT)
                            unicastMsgs(j, "abt");
                    return;
                }

            // TR2
            for (int i = 1; i <= totalProcess; i++)
                if (stateReqList[i] == STATE_COMMIT) {
                    performCommit();
                    for (int j = 1; j <= totalProcess; j++)
                        if (stateReqList[j] != STATE_COMMIT)
                            unicastMsgs(j, "c");
                    return;
                }

            // TR4
            for (int i = 1; i <= totalProcess; i++)
                if (stateReqList[i] == STATE_PRECOMMIT) {
                    broadcastMsgs("pc");
                    return;
                }

            // TR3
            performAbort();
            broadcastMsgs("abt");
        }
    }

    /**
     * Start termination protocol after electing new coordinator
     */
    private void termination() {
        stateReqAckNum = 0;
        Arrays.fill(hasRespond, false);
        Arrays.fill(stateReqList, STATE_NOTANS);
        broadcastMsgs("sr");
    }

    /**
     * Elect new coordinator
     */
    private void electCoordinator() {
        viewNumber = 1;
        while (!processAlive[viewNumber]) {
            viewNumber++;
            if (viewNumber > totalProcess) viewNumber = 1;
        }
        if (viewNumber == processId)
            broadcastMsgs("sr");
        else {
            currentState = STATE_SELETC;
            unicastMsgs(viewNumber, "ue");
            timer.start();
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
        voteNum++;
        hasRespond[senderId] = true;
        if (vote.equals("no")) {
            decision = 0;
            if (voteNum == totalProcess-1) {
                timer.stop();
                performAbort();
            }
        }
        else {
            if (!voteStats[senderId]) {
                voteStats[senderId] = true;
                if (voteNum == totalProcess-1 && decision == 1) {
                    timer.stop();
                    logWrite(STATE_PRECOMMIT);
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
        ackNum++;
        hasRespond[senderId] = true;
        if (!ackStats[senderId]) {
            if (ackNum == totalProcess-1) {
                timer.stop();
                logWrite(STATE_ACKED);
                broadcastMsgs("c");
                currentCommand = "# # " + currentCommand; // format the command
                performCommit();
            }
        }
    }

    /**
     * Perform COMMIT operation
     */
    public void performCommit() {
        if (currentCommand.equals("")) return;
        if (isRecover)
            logWrite(STATE_RECOVER+":"+currentCommand);
        else
            logWrite(STATE_COMMIT);

        String[] splits = currentCommand.split(" ");
        switch (splits[3]) {
            case "add":
                add(splits[4], splits[5]);
                break;
            case "e":
                edit(splits[4], splits[5], splits[6]);
                break;
            case "rm":
                remove(splits[4]);
                break;
            default:
                break;
        }

        currentState = STATE_COMMIT;
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
                    if (i != processId && voteStats[i])
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

    private void checkPartialMsg() {
        if (countingMsg) {
            this.msgCounter++;
            if (msgCounter == msgAllowed) {
                System.exit(0);
            }
        }
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
        //logWrite(PREFIX_ALIVEPROC+aliveProcessList());
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     * @param URL
     */
    public void voteAddCoordinator(String songName, String URL) {
        currentState = STATE_START;
        instanceNum ++;
        logWrite(PREFIX_START+instanceNum);
        if (playlist.containsKey(songName) || rejectNextChange) {
            logWrite(PREFIX_COMMAND + "# " + currentCommand);
            performAbort();
        } else {
            currentState = STATE_WAITVOTE;
            broadcastMsgs(instanceNum + " vr add " + songName + " " + URL);
            waitVote();
        }
        rejectNextChange = false;
    }

    public void voteAddParticipant(String songName) {
        if (playlist.containsKey(songName) || rejectNextChange) {
            performAbort();
            String msg = String.format("%d v no", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        } else {
            logWrite(PREFIX_VOTE + "YES");
            String msg = String.format("%d v yes", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        }
        rejectNextChange = false;
        checkPartialMsg();
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
            broadcastMsgs(instanceNum+" vr rm " + songName);
            waitVote();
        } else {
            logWrite(PREFIX_COMMAND + "# " + currentCommand);
            performAbort();
        }
        rejectNextChange = false;
    }

    public void voteRmParticipant(String songName) {
        if (playlist.containsKey(songName) && !rejectNextChange) {
            logWrite(PREFIX_VOTE + "YES");
            String msg = String.format("%d v yes", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        } else {
            performAbort();
            String msg = String.format("%d v no", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        }
        rejectNextChange = false;
        checkPartialMsg();
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
            broadcastMsgs(instanceNum+" vr e " + songName + " " + newSongName + " " + URL);
            waitVote();
        } else {
            logWrite(PREFIX_COMMAND + "# " + currentCommand);
            performAbort();
        }
        rejectNextChange = false;
    }

    public void voteEditParticipant(String songName, String newSongName) {
        if (playlist.containsKey(songName) && !playlist.containsKey(newSongName) && !rejectNextChange) {
            logWrite(PREFIX_VOTE + "YES");
            String msg = String.format("%d v yes", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        } else {
            performAbort();
            String msg = String.format("%d v no", processId);
            if (canSendMsg)
                netController.sendMsg(leader, msg);
        }
        rejectNextChange = false;
        checkPartialMsg();
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
        terminalLog("update <" + songName + "> to <" + newSongName + ", " + newSongURL + ">");
    }

    public void sendAcknowledge() {
        if (canSendMsg)
            unicastMsgs(leader, "ack");
        checkPartialMsg();
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
        String msg = String.format("%d %s", processId, instruction);
        if (canSendMsg)
            netController.sendMsg(destId, msg);
        checkPartialMsg();
    }

    /**
     * Broadcast to available partners
     * @param instruction
     */
    private void broadcastMsgs(String instruction) {
        for (int i = 1; i <= totalProcess; i++)
            if (i != processId) {
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
