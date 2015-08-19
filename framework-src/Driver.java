/**
 * Class Driver
 *
 * This is where the framework starts.
 * If this is framework bootstrap we do the following:
 * - create a Driver object
 * - read the configuration file
 * - if the current node is master start the master, else start the slave
 * Else If this is a startjob request we do the following:
 * - create a ClientDriver object
 * - read the configuration file
 * - startjob()
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Driver {

	private Communicator communicator;
	private String localHostName;
	private String masterHostName;
	private List<String> dataHostNames;
	private List<String> alldataNodes;
	private String HDFS_DIR;
	private int PORT;
	private int REGISTRY_PORT;
	private int numRecPerChunk;
	private int REPLICATION_FACTOR;
	private int MAXIMUM_MAPS;
	private int MAXIMUM_REDUCES;
	
	public Driver() {
		try {
			this.localHostName = InetAddress.getLocalHost().getHostName();
			dataHostNames = new ArrayList<String>();
			alldataNodes = new ArrayList<String>();
		} catch (UnknownHostException e) {
			System.out.println("Could not find local host.");
		}
	}
	
	public static void main(String[] args) {

		if(args.length<1) {
			System.out.println("Incorrect usage, please provide argument specifying if node is master"
					+ " or slave.");
			return;
		}
		
		else if(args[0].equals("startjob")) {
		        // create ClientDriver, readConfiguration and start the job
			ClientDriver clientdriver = new ClientDriver(args);
			clientdriver.readConfiguration();
			clientdriver.startJob();
		}
		
		else {	//start the driver, which starts up the nodes
			Driver driver = new Driver();
			driver.readConfiguration();
			
			if(args[0].equals("master")) {
				driver.beginMaster();
			} else if(args[0].equals("slave")) {
				driver.beginSlave();
			}
			
			driver.startCommandLine();
		}
	}

        /**
         * readConfiguration: Read the "config.txt" configuration file and initialize fields accordingly
         */
	private void readConfiguration() {
		try {
			BufferedReader br = new BufferedReader(new FileReader("config.txt"));
			String line = null;
			while( (line=br.readLine())!=null ) {
				String[] values = line.split("=");
				
				switch(values[0]) {
				
				case "MASTER_NODE":
					masterHostName=values[1];
					break;
				case "DATA_NODES":
					String[] nodes = values[1].split(";");
					for(String s: nodes) {
						alldataNodes.add(s);
						if(!s.equals(localHostName)) {
							dataHostNames.add(s);
						}					
					}
					break;
				case "PORT":
					this.PORT = Integer.parseInt(values[1]);
					break;
					
				case "REGISTRY_PORT":
					this.REGISTRY_PORT = Integer.parseInt(values[1]);
					break;
				case "HDFS_DIR":
					this.HDFS_DIR = values[1];
					break;
					
				case "NUM_RECORDS_PER_CHUNK":
					this.numRecPerChunk = Integer.parseInt(values[1]);
					break;
				case "REPLICATION_FACTOR" :
					this.REPLICATION_FACTOR = Integer.parseInt(values[1]);
					break;
					
				case "MAXIMUM_MAPS":
					this.MAXIMUM_MAPS = Integer.parseInt(values[1]);
					break;
					
				case "MAXIMUM_REDUCES":
					this.MAXIMUM_REDUCES = Integer.parseInt(values[1]);
					break;
					
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println("Could not find configuration file");
		} catch (IOException e) {
			System.out.println("Could not read from configuration file");
		}
	}

        /**
         * startCommandLine: Once the master or slave node functionality is up do nothing
         */
	private void startCommandLine() {
		while(true);
	}

        /**
         * beginMaster: create a communicator object for the master
         * register it with the registry
         * start the heart beat thread
         */
	private void beginMaster() {
		try {		
			LocateRegistry.createRegistry(REGISTRY_PORT);
			communicator = new Communicator(true, localHostName, masterHostName, dataHostNames, HDFS_DIR, 
					numRecPerChunk, REPLICATION_FACTOR, REGISTRY_PORT, MAXIMUM_MAPS, MAXIMUM_REDUCES, alldataNodes);
			Naming.rebind(("//"+masterHostName+":"+REGISTRY_PORT+"/communicator_"+masterHostName), communicator);
			communicator.startHeartBeatThread();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

        /**
         * beginSlave: create a communicator object for the slave
         * register it with the registry
         */
	private void beginSlave() {
		try {
			LocateRegistry.createRegistry(REGISTRY_PORT);
			communicator = new Communicator(false, localHostName, masterHostName, dataHostNames, HDFS_DIR, 
					numRecPerChunk, REPLICATION_FACTOR, REGISTRY_PORT, MAXIMUM_MAPS, MAXIMUM_REDUCES, alldataNodes);
			Naming.rebind(("//"+localHostName+":"+REGISTRY_PORT+"/communicator_"+localHostName), communicator);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

}
