package edu.utexas.cs.threepc;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.lang.Process;

public class Master {

    private List<Process> processList;
    private int totalProcess;
    private int leader;
    private int lastKilled;

    public Master() {
        processList = new ArrayList<Process>();
        totalProcess = 0;
    }

    public Master(String instruction_log) {

    }

    /**
     *
     * @param n number of processes
     */
    public void createProcesses(int n) {
        totalProcess = n;
        for (int i = 0; i < n; i++) {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "Worker.jar", ""+n, "1");
            try {
                processList.add(i, pb.start());

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     *
     * @param processId process id
     */
    public void kill(int processId) {
        Process p = processList.get(processId);
        p.destroy();
        processList.set(processId, null);
        lastKilled = processId;
    }

    public void killAll() {
        for (int i = 0; i < totalProcess; i++) {
            kill(i);
        }
    }

    public void killLeader() {
        kill(leader);
    }

    public void revive(int processId) {
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "Worker.jar", ""+processId, "0");
        try {
            processList.set(processId, pb.start());
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public void reviveLast() {
        revive(lastKilled);
    }

    public void reviveAll() {
        for (int i = 0; i < totalProcess; i++) {
            revive(i);
        }
    }

    public void partialMessage(int process_id, int num_of_messages) {

    }

    public void resumeMessages (int process_id) {

    }

    public void allClear() {

    }

    public void rejectNextChange(int process_id) {

    }

    public static void main(String[] args) {
	// write your code here


        while (true) {

        }
    }
}
