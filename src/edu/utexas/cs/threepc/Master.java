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
    private int delayTime;

    public Master() {
        processList = new ArrayList<Process>();
        processList.add(0, null);
        System.err.println("Master started");
        totalProcess = leader = lastKilled = 0;
        delayTime = 0;
    }

    /**
     * Create @param n processes
     * @param n number of processes
     */
    public void createProcesses(int n) {
        Process p;
        killAll();
        for (int i = 1; i <= n; i++) {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "./worker.jar", ""+i, "1").redirectErrorStream(true);
            try {
                p = pb.start();
                System.err.println("Process " + i + " [" + p + "] started");
                inheritIO(p.getInputStream(), System.err);
                processList.add(i, p);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        totalProcess = n;
        assignLeader(1);
    }

    /**
     * Redirect ProcessBuilder's output to System.err
     * @param src
     * @param dest
     */
    private static void inheritIO(final InputStream src, final PrintStream dest) {
        new Thread(new Runnable() {
            public void run() {
                Scanner sc = new Scanner(src);
                while (sc.hasNextLine()) {
                    dest.println(sc.nextLine());
                }
            }
        }).start();
    }

    /**
     * Used for debugging
     */
    public void printParameters() {
        System.err.print("process_num=" + totalProcess + ", leader_no=" + leader + ", last_killed_no=" + lastKilled + ", existed_process=[");
        boolean existed = false;
        for (int i = 1; i <= totalProcess; i++) {
            if (processList.get(i) != null) System.err.print(i + " ");
            existed = true;
        }
        if (!existed) System.err.println("null]");
        else System.err.println("]");
    }

    /**
     * Kill processId
     * @param processId process id
     */
    public void kill(int processId) {
        Process p = processList.get(processId);
        if (p == null) {
            System.err.println("Proces "+processId+" not exists");
        }
        else {
            p.destroy();
            processList.set(processId, null);
            System.err.println("Proces "+processId+" killed");
            lastKilled = processId;
        }
    }

    /**
     * Kill all processes
     */
    public void killAll() {
        for (int i = 1; i <= totalProcess; i++) {
            kill(i);
        }
    }

    /**
     * Kill coordinator
     */
    public void killLeader() {
        if (leader == 0 || leader > processList.size()) {
            System.err.println("Coordinator not exists");
            return;
        }

        Process p = processList.get(leader);
        if (p == null) {
            System.err.println("Proces "+leader+" not exists");
        }
        else {
            kill(leader);
        }
    }

    public void revive(int processId) {
        if (processList.get(processId) != null) {
            System.err.println("Proces "+processId+" alrealy exists");
        }
        else {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "./worker.jar", "" + processId, "0").redirectErrorStream(true);
            try {
                processList.set(processId, pb.start());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void reviveLast() {
        revive(lastKilled);
    }

    public void reviveAll() {
        for (int i = 1; i <= totalProcess; i++) {
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

    public void partialCommit(int second) {

    }

    /**
     * Delay time between each two continuous commands
     * @param second
     */
    public void delay(int second) {
        delayTime = second;
    }

    public void deathAfter(int numMessages, int processId) {

    }

    /**
     * Assign coordinator
     * @param processId
     */
    public void assignLeader(int processId) {
        if (processList.get(processId) == null) {
            System.err.println("Proces "+processId+" not exists");
        }
        else {
            leader = processId;
        }
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
            case "pc":
                int receiveProcessId = Integer.parseInt(splits[1]);
                m.partialCommit(receiveProcessId);
                break;
            case "d":
                int delayTime = Integer.parseInt(splits[1]);
                m.delay(delayTime);
                break;
            case "da":
                int numReceiveMessages = Integer.parseInt(splits[1]);
                int killSelfProcessId = Integer.parseInt(splits[2]);
                m.deathAfter(numReceiveMessages, killSelfProcessId);
                break;
            case "pp":
                m.printParameters();
                break;
            default:
                System.err.println("Cannot recognize this command: " + splits[0]);
                //System.exit(-1);
                break;
        }
    }

    public static void main(String[] args) {

        Master m = new Master();
        if (args.length != 0) {
            File f = new File("data/"+args[0]);
            try (BufferedReader br = new BufferedReader(new FileReader("data/command"))) {
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
