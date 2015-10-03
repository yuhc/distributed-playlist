package edu.utexas.cs.threepc;

import edu.utexas.cs.netutil.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

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
    private List<Boolean> processAlive;

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

        playlist = new HashMap<String, String>();
        try {
            DTLog = new File("log_" + process_id + ".txt");
            if (!DTLog.exists()) {
                DTLog.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        processAlive = new ArrayList<Boolean>();
        for (int i = 0; i <= totalProcess; i++) {
            processAlive.add(i, true);
        }

        buildSocket();
        getReceivedMsgs(netController);
    }

    public void processMessage(String message) {
        String[] splits = message.split(" ");

        int sender_id = Integer.parseInt(splits[0]);
        switch (splits[1]) {
            case "vr":
                break;
            case "v":
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
                vote_add_edit(splits[2]);
                break;
            case "rm":
                vote_rm(splits[2]);
                break;
            case "e":
                vote_add_edit(splits[2]);
                break;
            default:
                System.err.println("Cannot recognize this command: " + splits[0]);
                break;
        }
    }

    /**
     * Respond to VOTE_REQ
     * @param songName
     */
    public void vote_add_edit(String songName) {
        if (playlist.containsKey(songName)) {
            netController.sendMsg(leader, String.format("%d v no", processId));
        }
        else {
            netController.sendMsg(leader, String.format("%d v yes", processId));
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
    public void vote_rm(String songName) {
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

    /**
     * Receive messages
     * @param netController
     */
    private static void getReceivedMsgs(final NetController netController) {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    List<String> receivedMsgs = new ArrayList<String>(netController.getReceivedMsgs());
                    for (int i = 0; i < receivedMsgs.size(); i++) {
                        System.err.println(String.format("[MASTER] receive \"%s\"", receivedMsgs.get(i)));
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
        System.err.println("[process "+processId+"] started");
        w.netController.sendMsg(0, "aaa");

    }


}
