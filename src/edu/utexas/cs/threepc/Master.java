package edu.utexas.cs.threepc;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.lang.Process;
import java.util.Scanner;

public class Master {

    private List<Process> processList;
    private int totalProcess;
    private int leader;
    private int lastKilled;

    public Master() {
        processList = new ArrayList<Process>();
        totalProcess = 0;
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

    public void partialMessage(int processId, int numMessages) {

    }

    public void resumeMessages (int processId) {

    }

    public void allClear() {

    }

    public void rejectNextChange(int processId) {

    }

    public static void handleRequest(Master m, String req) {
        String[] splits = req.split(" ");
        switch (splits[0]) {
            case "cp":
                int numProcess = Integer.parseInt(splits[1]);
                m.createProcesses(numProcess);
                break;
            case "k":
                int killProcessId = Integer.parseInt(splits[1]);
                m.kill(killProcessId);
                break;
            case "ka":
                m.killAll();
                break;
            case "kl":
                m.killLeader();
                break;
            case "r":
                int reviveProcessId = Integer.parseInt(splits[1]);
                m.revive(reviveProcessId);
                break;
            case "rl":
                m.reviveLast();
                break;
            case "ra":
                m.reviveAll();
                break;
            case "pm":
                int pauseProcessId = Integer.parseInt(splits[1]);
                int numMessages = Integer.parseInt(splits[2]);
                m.partialMessage(pauseProcessId, numMessages);
                break;
            case "rm":
                int resumeProcessId = Integer.parseInt(splits[1]);
                m.resumeMessages(resumeProcessId);
                break;
            case "ac":
                m.allClear();
                break;
            case "rnc":
                int rejectProcessId = Integer.parseInt(splits[1]);
                m.rejectNextChange(rejectProcessId);
                break;
            default:
                System.err.println("Cannot recognize this command: " + splits[0]);
                System.exit(-1);
                break;
        }
    }

    public static void main(String[] args) {
	// write your code here
        Master m = new Master();
        if (args.length != 0) {
            File f = new File(args[0]);
            try (BufferedReader br = new BufferedReader(new FileReader("command.txt"))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    handleRequest(m, line);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Scanner sc = new Scanner(System.in);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                handleRequest(m, line);
            }
        }
    }
}
