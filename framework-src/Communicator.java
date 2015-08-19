/**
 * Communicator: The communicator class is the heart of all the
 * communication between master and participant nodes as well as
 * between individual participant nodes.
 *
 * Every node (MasterNode/DataNode) in the system has a communicator object associated with it.
 * This communicator object is exported via the RMI registry.
 * If node A wants to talk to node B, A looks up the communicator object
 * for B and uses it to perform remote method invocations.
 */
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Communicator extends UnicastRemoteObject implements CommunicatorInterface {
    
	private static int dataNodeIndex = 0;

        /* Am I master? */
	private boolean master;
        /* node's local hostname */
	private String localHostName;
        /* master's hostname */
	private String masterHostName;
        /* List of hostnames of data nodes */
	private List<String> dataHostNames;
	private List<String> alldataNodes;

	private Node node;
        /* Global HDFS location */
	private String HDFS_DIR;
        /* number of records per chunk */
	private int numRecPerChunk;
        /* the replication factor supported by the framework */
	private int REPLICATION_FACTOR;
        /* port on which the registry is running */
	private int REGISTRY_PORT;
        /* maximum map operations supported per node */
	private int MAXIMUM_MAPS;
        /* maximum reduce operations supported per node */
	private int MAXIMUM_REDUCES;
        /* lock to protect access to the list of data nodes */
	private Lock alldataNodesLock;


        /**
         * Communicator constructor
         * @param master
         * @param localHostName
         * @param masterHostName
         * @param dataHostNames
         * @param HDFS_DIR
         * @param numRecPerChunk
         * @param REPLICATION_FACTOR
         * @param REGISTRY_PORT
         * @param MAXIMUM_MAPS
         * @param MAXIMUM_REDUCES
         * @param alldataNodes
         * @throws RemoteException
         */
	public Communicator(boolean master, String localHostName, String masterHostName,
			    List<String> dataHostNames, String HDFS_DIR, int numRecPerChunk,
			    int REPLICATION_FACTOR, int REGISTRY_PORT, int MAXIMUM_MAPS, int MAXIMUM_REDUCES
			    , List<String> alldataNodes) throws RemoteException {
		this.master = master;
		this.setLocalHostName(localHostName);
		this.masterHostName = masterHostName;
		this.dataHostNames = dataHostNames;
		this.HDFS_DIR = HDFS_DIR;
		this.REPLICATION_FACTOR = REPLICATION_FACTOR;
		this.REGISTRY_PORT = REGISTRY_PORT;
		this.MAXIMUM_MAPS = MAXIMUM_MAPS;
		this.MAXIMUM_REDUCES = MAXIMUM_REDUCES;
		this.setNumRecPerChunk(numRecPerChunk);
		this.alldataNodes = alldataNodes;
		this.alldataNodesLock = new ReentrantLock();
		createNode();
        }

        /**
         * createNode: create a MasterNode/DataNode object based on whether this is a
         * master node or not.
         */
	private void createNode() {
		if(master) {
			node = new MasterNode(this);
		}
		else {
			node = new DataNode(this);
		}
		
	}

        /**
         * isMaster:
         * @return returns true if this is the master
         * false otherwise
         */
	public boolean isMaster() {
		return master;
	}

        /**
         * submitJob: This function performs the following tasks:
         * 1) Store the jobconf passed in the Master's jobConfList
         * 2) Request the master node to assign a new unique jobID to this job
         * 3) Correctly set the number of reducers for the job depending on
         * how many DataNodes are currently alive in the system
         * 4) Notify all DataNodes to create job directory and intermediate file
         * directory
         * 5) Send client JAR file to all the DataNodes
         * 6) Split all the input files from the input directory specified by the user
         * 7) Finally inform the master to schedule the job
         * @param jobconf: Job Configuration of the job to be submitted
         */
	@Override
	public void submitJob(JobConfiguration jobconf) {
		if (isMaster()) {
			MasterNode mast = ((MasterNode) node);
                        /* store the job configuration for use during the cleanup/restart */
			mast.storeJob(jobconf);
                        /* Get a new unique job ID for this job */
			mast.assignJobID(jobconf);

			acquireDataNodesLock();
                                int numDataNodes = getAllDataNodes().size();
                                /*
                                If the number of reducers specified by the user is greater than
                                the number of current alive nodes then set the number of reducers
                                 to the number of current alive nodes
                                 */
                                if (jobconf.getNumberOfReducers() > numDataNodes) {
                                        jobconf.setNumberOfReducers(numDataNodes);
                                }
			releaseDataNodesLock();

                        /* create a job directory on the master node */
			jobconf.setJobDir(HDFS_DIR+"/"+jobconf.getJobName()+"-"+Integer.toString(jobconf.getJobID()));
			createJobIDDir(jobconf);
		
			for(String host: dataHostNames) {
		                try {
                                        Registry registry = LocateRegistry.getRegistry(host, REGISTRY_PORT);
                                        CommunicatorInterface communicator = (CommunicatorInterface)
                                                registry.lookup("communicator_" + host);
                                        /** create job directory on the DataNode **/
                                        communicator.createJobIDDir(jobconf);
                                        /** create intermediate files directory on the DataNode **/
                                        communicator.createIntermediateDir(jobconf);
                                        /** send client JAR file to the DataNode **/
                                        communicator.receiveJarFile(jobconf);
                                } catch (RemoteException e) {
                                        //e.printStackTrace();
                                } catch (NotBoundException e) {
                                        System.out.println("Communicator object for " + host + " could not be found.");
				}
			}
			
			List<File> inputFileDescriptors = jobconf.getinputFileDescriptors();

                        /**
                         * Copy over all the input files from the AFS to HDFS.
                         * We assume all the jobs are run from the master. Here we simply copy over
                         * the files from the client specified input directory to master's HDFS location..
                         * After this the master can go ahead and split the input files into chunks and send
                         * the chunks to the DataNodes
                         */
			for(int i = 0;i < inputFileDescriptors.size(); i++) {
                                receiveInputFile(jobconf, inputFileDescriptors.get(i));
				mast.splitInputFile(jobconf, jobconf.getJobDir() + "/" + inputFileDescriptors.get(i).getName());
			}

                        /* Ask the master to schedule the job */
			mast.scheduleJob(jobconf);
		} else {
                        /**
                         * We come in here if the user starts a MapReduce job on a DataNode.
                         * We support the functionality of starting a job from any node in the system
                         * but we currently do not use it.
                         */
			try {
				Registry registry = LocateRegistry.getRegistry(masterHostName, REGISTRY_PORT);
	        	        CommunicatorInterface communicator =
                                        (CommunicatorInterface) registry.lookup("communicator_" + masterHostName);
				communicator.submitJob(jobconf);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				System.out.println("Communicator object for " + masterHostName + " could not be found.");
			}
		}
	}

        /**
         * sendOneChunk: Worker function called by sendChunkWithReplication()
         * @param host: host to which this chunk is to be sent to
         * @param jobConf: job configuration of the job this chunk belongs to
         * @param chunk: the chunk that needs to be sent
         */
        private void sendOneChunk(String host, JobConfiguration jobConf, Chunk chunk) {
                Registry registry;
                try {
                        registry = LocateRegistry.getRegistry(host, REGISTRY_PORT);
                        CommunicatorInterface communicator =
                                (CommunicatorInterface) registry.lookup("communicator_" + (host));
                        communicator.receiveChunk(jobConf, chunk);
                } catch (RemoteException e) {
                        //e.printStackTrace();
                } catch (NotBoundException e) {
                        //e.printStackTrace();
                }
        }

        /**
         * sendChunkWithReplication: send one chunk at a time to multiple nodes
         * to maintain the replication factor
         * @param jobconf: Job Configuration of the job
         * @param chunk: Chunk to be sent and replicated
         * @param namespace: Global namespace object
         */
	public void sendChunkWithReplication(JobConfiguration jobconf, Chunk chunk, NameSpace namespace) {
		int i = 0;
		String host = null;
		List<String> nodes = new ArrayList<String>();
		
		while(i < REPLICATION_FACTOR) {
                        /* dataNodeIndex counter should get wrapped around to 0
                        once we reach the last DataNode. The replication should then
                        again begin from DataNode 0
                         */
			if(dataNodeIndex >= dataHostNames.size()) {
				dataNodeIndex = 0;
			}
			host = dataHostNames.get(dataNodeIndex) ;
			sendOneChunk(host, jobconf, chunk);
			nodes.add(host);
			i++;
			dataNodeIndex++;
		}

                /*
                Add a list of all the nodes where we sent this chunk to the namespace object.
                This helps us in tracking which nodes have a chunk
                 */
		namespace.addNodeListToMap2(chunk.getChunkID(), nodes);
	}

        /**
         * createJobIDDir: Create a directory for this job in HDFS
         * @param jobconf: job's configuration
         */
	public void createJobIDDir(JobConfiguration jobconf) {
		File dir = new File(jobconf.getJobDir());
		dir.mkdir();
	}

        /**
         * receiveJarFile: receive the client JAR file and store it in the
         * job directory created for this job
         * @param jobconf: job's configuration
         */
	@Override
	public void receiveJarFile(JobConfiguration jobconf) {
		try {
			Files.write(Paths.get(jobconf.getJobDir() + "/client.jar"), jobconf.getJarData());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

        /**
         * receiveInputFile: Receive input file from the client specified input directory
         * and store in our HDFS location.
         * This method will only be called from the master node.
         * This receiving of input file does not involve any network communication or reading
         * of the input file into the memory. Since the jobs are always started from the master
         * the input files are available locally to the master. We just copy them over to our HDFS
         * location.
         * @param jobconf: : job's configuration
         * @param inputFile
         */
	@Override
	public void receiveInputFile(JobConfiguration jobconf, File inputFile) {
                Process p;
		try {
                        String command = "cp " + inputFile.getAbsolutePath() + " " +
                                         jobconf.getJobDir()+"/"+inputFile.getName();
                        p = Runtime.getRuntime().exec(command);
                        p.waitFor();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
                        e.printStackTrace();
                }
        }

        /**
         * getNumRecPerChunk:
         * @return returns number of records per chunk
         */
	public int getNumRecPerChunk() {
		return numRecPerChunk;
	}

        /**
         * setNumRecPerChunk: set the number of records per chunk
         * @param numRecPerChunk
         */
	public void setNumRecPerChunk(int numRecPerChunk) {
		this.numRecPerChunk = numRecPerChunk;
	}

        /**
         * receiveChunk: Receive a chunk sent by the master node and store it in the HDFS in
         * the directory created for this job
         * @param jobconf: : job's configuration
         * @param chunk: chunk received from master
         * @throws RemoteException
         */
	@Override
	public void receiveChunk(JobConfiguration jobconf, Chunk chunk) throws RemoteException {
		String chunkFileName = jobconf.getJobDir()+"/chunk-"+chunk.getChunkID();
		Path path = Paths.get(chunkFileName);
		if(!Files.exists(path)) {
                        // write out the chunk using the RecordWriter
			RecordWriter rw = new RecordWriter(jobconf.getJobDir() + "/chunk-" + chunk.getChunkID());
			rw.writeChunk(chunk);
			rw.close();
		}		
	}

        /**
         * getMAXIMUM_MAPS: maximum maps supported per DataNode
         * @return
         */
	public int getMAXIMUM_MAPS() {
		return MAXIMUM_MAPS;
	}

        /**
         * getMAXIMUM_REDUCES: maximum reduces supported per DataNode
         * @return
         */
	public int getMAXIMUM_REDUCES() {
		return MAXIMUM_REDUCES;
	}

        /**
         * getREGISTRY_PORT: port on which the registry is running
         * @return
         */
	public int getREGISTRY_PORT() {
		return REGISTRY_PORT;
	}

        /**
         * getMapLoad: returns the number of mapper tasks currently running on a DataNode.
         * We use this information for scheduling.
         * @return
         * @throws RemoteException
         */
	@Override
	public int getMapLoad() throws RemoteException {
		DataNode localNode = (DataNode) node;
		return localNode.getMapLoad();
	}

        /**
         * performMap: forward a map request from Master to a DataNode
         * @param mapTask: Task which needs to be mapped
         * @throws RemoteException
         */
	@Override
	public void performMap(MapTask mapTask) throws RemoteException {
		DataNode localNode = (DataNode) node;
		localNode.performMap(mapTask);
	}

        /**
         * createIntermediateDir: create a directory for temporarily placing
         * intermediate files created by the mapper tasks
         * @param jobconf: : job's configuration
         * @throws RemoteException
         */
	@Override
	public void createIntermediateDir(JobConfiguration jobconf)
			throws RemoteException {
		File dir = new File(jobconf.getJobDir() + "-intermediate");
		dir.mkdir();
		
	}

        /**
         * getDataNodeList: get the list of currently alive DataNodes
         * This list is always kept up-to-date
         * @return
         */
	public List<String> getDataNodeList() {
		return dataHostNames;
	}

        /**
         * receiveIntermediateFile: Get the sorted intermediate file which are mapped to this
         * DataNode and place them in the reducerfiles directory for that job
         * @param jobConf: job's configuration
         * @param intermediateData: intermediate file data
         * @param fileName: name of the intermediate file
         * @throws RemoteException
         */
	@Override
	public void receiveIntermediateFile(JobConfiguration jobConf, byte[] intermediateData,
                                            String fileName) throws RemoteException {
		Path path = Paths.get(jobConf.getJobDir() + "-reducerfiles/");
		if(!Files.exists(path)) {
			File dir = new File(jobConf.getJobDir() + "-reducerfiles/");
			dir.mkdir();
		}
		
		
		try {
			Files.write(Paths.get(jobConf.getJobDir() + "-reducerfiles/" + fileName),
                                    intermediateData);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

        /**
         * mapTaskCompleted: Notify the masterNode that a mapper task is completed
         * @param mapTask: mapTask that got completed
         * @throws RemoteException
         */
        @Override
        public void mapTaskCompleted(MapTask mapTask) throws RemoteException {
                MasterNode localNode = (MasterNode) node;
                localNode.removeMapperTask(mapTask.getJobConf().getJobID(), mapTask);
        }

        /**
         * reduceTaskCompleted: Notify the masterNode that a reducer task is completed
         * @param reduceTask: reduceTask that got completed
         * @throws RemoteException
         */
        @Override
        public void reduceTaskCompleted(ReduceTask reduceTask) throws RemoteException {
                MasterNode localNode = (MasterNode) node;
                localNode.removeReducerTask(reduceTask);
        }

        /**
         * Getter and setter for localHostName
         * @return
         */
        public String getLocalHostName() {
		return localHostName;
	}

	public void setLocalHostName(String localHostName) {
		this.localHostName = localHostName;
	}

        /**
         * performReduce: forward a reduce request from Master to a DataNode
         * @param reduceTask
         * @throws RemoteException
         */
	@Override
	public void performReduce(ReduceTask reduceTask) throws RemoteException {
		DataNode localNode = (DataNode) node;
		localNode.performReduce(reduceTask);
	}

        public String getMasterHostName() {
            return masterHostName;
        }

        public List<String> getAllDataNodes() {
        return alldataNodes;
        }

        /**
         * grabChunk: read a chunk into memory
         * This function is called when we want to transfer a chunk to a DataNode which
         * doesn't already have it.
         * @param jobConf: : job's configuration
         * @param chunkID: chunkID of the chunk
         * @return
         * @throws RemoteException
         */
	@Override
	public Chunk grabChunk(JobConfiguration jobConf, int chunkID) throws RemoteException {
		String nextRecord;
		RecordReader rr = new RecordReader(jobConf.getJobDir() + "/chunk-" + chunkID);
		Chunk chunk = new Chunk(chunkID);

                /**
                 * read the chunk one record at a time using the RecordReader
                 */
		while ((nextRecord = rr.getNextRecord()) != null) {
			chunk.addRecordToChunk(nextRecord);
		}
		rr.close();
		return chunk;
	}

        /**
         * startHeartBeatThread: start the heart beat thread to monitor status of all
         * the DataNodes. Only called by the master
         */
	public void startHeartBeatThread() {
		MasterNode localNode = (MasterNode) node;
		localNode.startHeartBeatThread();
	}

        /**
         * getStatus: send an "alive" status to the Master
         * @return
         * @throws RemoteException
         */
	@Override
	public String getStatus() throws RemoteException {
		return "alive";
	}

        /**
         * mapTaskFailed: notify the master that a map task has failed
         * @param task: task that failed
         * @param e: the exception due to which the map task failed
         * @throws RemoteException
         */
	@Override
	public void mapTaskFailed(MapTask task, Exception e) throws RemoteException {
		MasterNode localNode = (MasterNode) node;
                localNode.mapTaskFailed(task, e);
	}

        /**
         * reduceTaskFailed: notify the master that a reduce task failed
         * @param task: the reduce task that failed
         * @param e: Exception due to which the reduce task failed
         * @throws RemoteException
         */
	@Override
	public void reduceTaskFailed(ReduceTask task, Exception e) throws RemoteException {
		MasterNode localNode = (MasterNode) node;
                localNode.reduceTaskFailed(task, e);
	}

        /**
         * abortJob: Abort a job on this DataNode
         * @param jobID: jobID of the job to be aborted
         * @throws RemoteException
         */
	@Override
	public void abortJob(int jobID) throws RemoteException {
		DataNode localNode = (DataNode) node;
                localNode.abortJob(jobID);
	}

        /**
         * removeFailedNode: remove the failed node from the list of currently alive
         * DataNodes
         * @param failedNode: failed node to be removed
         * @throws RemoteException
         */
	@Override
	public void removeFailedNode(String failedNode) throws RemoteException {
		acquireDataNodesLock();
		        this.alldataNodes.remove(failedNode);
		releaseDataNodesLock();
		this.dataHostNames.remove(failedNode);
	}
	
	public void acquireDataNodesLock() {
		alldataNodesLock.lock();
	}

	public void releaseDataNodesLock() {
		alldataNodesLock.unlock();
	}

        /**
         * fsCleanup: Call local DataNode's fsCleanup function
         * @param jobConfiguration
         * @throws RemoteException
         */
	@Override
	public void fsCleanup(JobConfiguration jobConfiguration) throws RemoteException {
		DataNode localNode = (DataNode) node;
                localNode.fsCleanup(jobConfiguration);
	}
}
