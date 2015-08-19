/**
 * CommunicatorInterface:
 * Declares all the methods that are used as part of the communication
 * between either the master and a participant node or between two
 * participant nodes
 *
 * These are a set of functions which can be invoked from a remote JVM
 * and hence CommunicatorInterface extends the Remote interface
 */

import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;


public interface CommunicatorInterface extends Remote {

	public void submitJob(JobConfiguration jobconf) throws RemoteException;

	public void receiveJarFile(JobConfiguration jobconf) throws RemoteException;

	public void receiveInputFile(JobConfiguration jobconf, File inputFile) throws RemoteException;
	
	public void createJobIDDir(JobConfiguration jobconf) throws RemoteException;

	public void receiveChunk(JobConfiguration jobconf, Chunk chunk) throws RemoteException;

	public int getMapLoad() throws RemoteException;

	public void performMap(MapTask mapTask)  throws RemoteException;
	
	public void performReduce(ReduceTask reduceTask) throws RemoteException;

	public void createIntermediateDir(JobConfiguration jobconf) throws RemoteException;

	public void receiveIntermediateFile(JobConfiguration jobConf, byte[] intermediateData, String fileName) throws RemoteException;

	public void mapTaskCompleted(MapTask mapTask) throws RemoteException;

        public void reduceTaskCompleted(ReduceTask reduceTask) throws RemoteException;

	public Chunk grabChunk(JobConfiguration jobconf, int chunkID) throws RemoteException;
	
	public void sendChunkWithReplication(JobConfiguration jobconf, Chunk chunk, NameSpace namespace) throws RemoteException;

	public String getStatus() throws RemoteException;

	public void mapTaskFailed(MapTask task, Exception e) throws RemoteException;

	public void reduceTaskFailed(ReduceTask task, Exception e) throws RemoteException;

	public void abortJob(int jobID) throws RemoteException;

	public void removeFailedNode(String failedNode) throws RemoteException;

	public void fsCleanup(JobConfiguration jobConfiguration) throws RemoteException;
   
}
