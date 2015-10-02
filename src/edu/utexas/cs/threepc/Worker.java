package edu.utexas.cs.threepc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by zhitingz on 10/2/15.
 */
public class Worker {
    private int process_id;
    private int viewNumber;
    private File DTLog;
    private int messageCounter;

    public Worker(int process_id) {
        this.process_id = process_id;
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

    }

    public void remove(String songName) {

    }

    public void edit(String songName, String newSongName, String newSongURL) {

    }

    public static void main(String args[]) {

    }


}
