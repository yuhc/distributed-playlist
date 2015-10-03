package edu.utexas.cs.threepc;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Note: maybe use sock.connect to set a timeout.
 */
public class Worker {
    private int processId;
    private int viewNumber;
    private File DTLog;
    private int messageCounter;
    private Map<String, String> map;

    public Worker(int process_id) {
        this.processId = process_id;
        map = new HashMap<String, String>();
        try {
            DTLog = new File("log_" + process_id + ".txt");
            if (!DTLog.exists()) {
                DTLog.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public static void main(String args[]) {
        int processId = Integer.parseInt(args[0]);
        Worker w = new Worker(processId);
        System.err.println(processId);

    }


}
