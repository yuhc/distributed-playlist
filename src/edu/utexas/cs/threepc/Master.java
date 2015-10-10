package edu.utexas.cs.threepc;

import edu.utexas.cs.netutil.*;

import java.io.*;
import java.util.*;
import java.lang.Process;

public class Master {

    private List<Process> processList;
    private int   totalProcess;
    private int   leader;
    private int   lastKilled;
    private int   delayTime;

    final String  hostName = "127.0.0.1";
    final int     basePort = 9000;

    private NetController netController;

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
        if (n <= 0) {
            System.err.println("Wrong parameter");
            return;
        }

        Process p;
        if (totalProcess > 0) killAll();
        processList.add(0, null);
        totalProcess = n;

        for (int i = 1; i <= n; i++) {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "./worker.jar", ""+i, ""+totalProcess, hostName, ""+basePort, "1", "0").redirectErrorStream(true);
            try {
                p = pb.start();
                System.err.println("Process " + i + " [" + p + "] started");
                inheritIO(p.getInputStream(), System.err);
                processList.add(i, p);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assignLeader(1);

        buildSocket();
        getReceivedMsgs(netController);
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
     * Receive messgaes
     * @param netController
     */
    private void getReceivedMsgs(final NetController netController) {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    List<String> receivedMsgs = new ArrayList<String>(netController.getReceivedMsgs());
                    for (int i = 0; i < receivedMsgs.size(); i++) {
                        System.err.println(String.format("[MASTER] receive %s", receivedMsgs.get(i)));
                        if (receivedMsgs.get(i).endsWith("nl")) {
                            leader = Integer.parseInt(receivedMsgs.get(i).split(" ")[0]);
                        }
                    }
                }
            }
        }).start();
    }

    /**
     * Print all playlists of available workers
     */
    public void printLists() {
        netController.sendMsg(leader, "0 pl");
    }
    
    /**
     * Used for debugging
     */
    public void printParameters() {
        System.err.print("process_num=" + totalProcess + ", leader_no=" + leader + ", last_killed_no=" + lastKilled + ", existed_process=[ ");
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
        netController.shutdown();
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
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "./worker.jar", "" + processId, "0", hostName, "" + basePort, "" + leader, "1").redirectErrorStream(true);
            try {
                Process p = pb.start();
                System.err.println("Process " + processId + " [" + p + "] revived");
                inheritIO(p.getInputStream(), System.err);
                processList.set(processId, p);
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

    public void partialMessage(final int processId, int numMessages) {
        netController.sendMsg(processId, "0 pm "+numMessages);
        new Thread(new Runnable() {
            public void run() {
                try {
                    processList.get(processId).waitFor();
                } catch (InterruptedException e) {

                }
                System.err.println("Process " + processId + " halted");
                processList.set(processId, null);
            }
        }).start();
    }

    public void resumeMessages (int processId) {
        revive(processId);
    }

    public void allClear() {
        netController.sendMsg(leader, "0 ac");
    }

    public void rejectNextChange(int processId) {
        netController.sendMsg(processId, "0 rnc");
    }

    public void add(String songName, String URL) {
        netController.sendMsg(leader, "0 add "+songName+" "+URL);
    }

    public void remove(String songName) {
        netController.sendMsg(leader, "0 rm "+songName);
    }

    public void edit(String songName, String newSongName, String newURL) {
        netController.sendMsg(leader, "0 e "+songName+" "+newSongName+" "+newURL);
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
            System.err.println("Proces "+processId+" becomes coordinator");
        }
    }

    public void buildSocket() {
        Config config = null;
        try {
            config = new Config(0, totalProcess, hostName, basePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        netController = new NetController(config);

    }

    public static void handleRequest(Master m, String req) {
        String[] splits = req.split(" ");

        switch (splits[0]) {
            case "cp":
                try {
                    int numProcess = Integer.parseInt(splits[1]);
                    m.createProcesses(numProcess);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Wrong parameter");
                }
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
                try {
                    int pauseProcessId = Integer.parseInt(splits[1]);
                    int numMessages = Integer.parseInt(splits[2]);
                    m.partialMessage(pauseProcessId, numMessages);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Wrong parameter");
                }
                break;
            case "rm":
                int resumeProcessId = Integer.parseInt(splits[1]);
                m.resumeMessages(resumeProcessId);
                break;
            case "ac":
                m.allClear();
                break;
            case "rnc": // rejectNextChange
                try {
                    int rejectProcessId = Integer.parseInt(splits[1]);
                    m.rejectNextChange(rejectProcessId);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Wrong parameter");
                }
                break;
            case "add":
            case "a":
                try {
                    m.add(splits[1], splits[2]);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Wrong parameter");
                }
                break;
            case "remove":
            case "rmv":
                m.remove(splits[1]);
                break;
            case "edit":
            case "e":
                try {
                    m.edit(splits[1], splits[2], splits[3]);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Wrong parameter");
                }
                break;
            case "pp": // printParameters
                m.printParameters();
                break;
            case "pl": // printLists
                m.printLists();
            	break;
            case "quit":
            case "q":  // quit
                m.killAll();
                System.exit(0);
            default:
                System.err.println("Cannot recognize this command: " + splits[0]);
                break;
        }
    }

    public static void main(String[] args) {

        Master m = new Master();
        if (args.length != 0) {
            File f = new File("data/"+args[0]);
            if (!f.exists()) {
                System.err.println("Cannot find command file " + args[0]);
                System.exit(-1);
            }
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    handleRequest(m, line);
                }
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
