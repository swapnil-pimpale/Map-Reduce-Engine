/**
 * ClientDriver:
 * When a client submits a MapReduce job he creates a ClientDriver object
 * and the job is started using the startJob method of this class.
 *
 * The ClientDriver object stores the parameters that the client passes in
 * through the command line. These parameters/arguments include the client
 * JAR path, input directory path, output directory path, job name and the
 * number of reducers. The number of output files will be exactly the same
 * as the number of reducers specified by the client/user.
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;


public class ClientDriver {

	private String jarFilePath;
	private String inputDirectoryPath;
	private String outputDirectoryPath;
	private String jobName;
	private String localHostName;
	private int REGISTRY_PORT;
	private int numberOfReducers;

        /**
         * ClientDriver constructor: stores all the command line arguments in
         * the object
         * @param args
         */
	public ClientDriver(String[] args) {
		jobName = args[1];
		jarFilePath = args[2];
		inputDirectoryPath = args[3];
		outputDirectoryPath = args[4];
		numberOfReducers = Integer.parseInt(args[5]);
		try {
			this.localHostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			System.out.println("Could not find local host.");
		}
	}

        /**
         * startJob: Creates a jobconf with all the information provied by the client and submits
         * the job to the master.
         * Since jobs can only be submitted from the master, we get the master's/localhost's communicator
         * here.
         */
	public void startJob() {
		try {
			Registry registry = LocateRegistry.getRegistry(localHostName, REGISTRY_PORT);
			CommunicatorInterface communicator =
                                (CommunicatorInterface) registry.lookup("communicator_" + localHostName);
			JobConfiguration jobconf = new JobConfiguration(jobName, jarFilePath,
                                inputDirectoryPath, outputDirectoryPath, numberOfReducers);
			communicator.submitJob(jobconf);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			System.out.println("Communicator object for " + localHostName + " could not be found.");
		}
	}

        /**
         * readConfiguration: read the config file to get the number of DataNodes
         * and the port on which the registry is running
         */
	public void readConfiguration() {
		try {
			BufferedReader br = new BufferedReader(new FileReader("config.txt"));
			String line = null;
			while( (line=br.readLine())!=null ) {
				String[] values = line.split("=");
				
				switch(values[0]) {		
				case "DATA_NODES":
					String[] nodes = values[1].split(";");
					if(nodes.length < numberOfReducers) {
						numberOfReducers = nodes.length;
					}
					break;
				case "REGISTRY_PORT":
					this.REGISTRY_PORT = Integer.parseInt(values[1]);
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

}
