/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 * @modified by Hangchen Yu
 */

package edu.utexas.cs.netutil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.logging.Logger;

public class Config {

	/**
	 * Loads config from a file.  Optionally puts in 'procNum' if in file.
	 * See sample file for syntax
	 * @param no
	 * @param nProc
	 * @param host
	 * @param basePort
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Config(int no, int nProc, String host, int basePort) throws FileNotFoundException, IOException {
		procNo = no;
		logger = Logger.getLogger("data/process"+procNo+".log");

		numProcesses = nProc;
		addresses = new InetAddress[numProcesses+1];
		ports = new int[numProcesses+1];
		for (int i = 0; i <= numProcesses; i++) {
			ports[i] = basePort + i;
			addresses[i] = InetAddress.getByName(host);
		}
	}

	/**
	 * Array of addresses of other hosts.  All hosts should have identical info here.
	 */
	public InetAddress[] addresses;
	

	/**
	 * Array of listening port of other hosts.  All hosts should have identical info here.
	 */
	public int[] ports;
	
	/**
	 * Total number of hosts
	 */
	public int numProcesses;
	
	/**
	 * This hosts number (should correspond to array above).  Each host should have a different number.
	 */
	public int procNo;
	
	/**
	 * Logger.  Mainly used for console printing, though be diverted to a file.
	 * Verbosity can be restricted by raising level to WARN
	 */
	public Logger logger;
}
