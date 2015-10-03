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
    private int  viewNumber;
    private File DTLog;
    private int  messageCounter;
    private Map<String, String> map;
    private List<Boolean> processAlive;

    private String hostName;
    private int    basePort;
    private int    reBuild;
    private NetController netController;

    public Worker(int process_id, int total_process, String host_name, int base_port, int rebuild) {
        processId = process_id;
        totalProcess = total_process;
        hostName = host_name;
        basePort = base_port;
        reBuild = rebuild;

        map = new HashMap<String, String>();
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

    public void add(String songName, String URL) {
        map.put(songName, URL);
    }

    public void remove(String songName) {
        map.remove(songName);
    }

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
                        System.err.println(String.format("[MASTER] receive %s", receivedMsgs.get(i)));
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
        int    reBuild = Integer.parseInt(args[4]);

        Worker w = new Worker(processId, totalProcess, hostName, basePort, reBuild);
        System.err.println("[process "+processId+"] started");
        w.netController.sendMsg(0, "aaa");

    }


}
