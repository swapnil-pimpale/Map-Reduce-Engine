/**
 * Class DataNode: This class defines methods and fields required to perform
 * Map and Reduce tasks. The DataNode is the participant/slave node.
 * The master gets all the job done through these DataNodes.
 *
 * The job of a DataNode is as follows:
 * 1) accept Map and Reduce requests from the master
 * 2) start a new thread/executor service for executing each of them separately
 * 3) Act as ThreadCompletionListener to monitor the state of the individual tasks
 * 4) Retry a failed map/reduce task once
 * 5) Notify master of successful completion or failure of individual tasks
 * 6) Send "alive" status whenever the master polls
 * 7) Keeps it's data structures in sync with the ones present on master
 * 8) Cleanup the in-memory state and on-disk temporary/intermediate files for
 * a job on completion
 */

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataNode extends Node implements ThreadCompletionListener{

        /** list of map tasks running on this node **/
	private List<MapTask> mapTasks;
        /** lock to protect access to mapTasks **/
	private Lock mapLock;
        /** list of reduce tasks running on this node **/
	private List<ReduceTask> reduceTasks;
        /** lock to protect access to reduceTasks **/
	private Lock reduceLock;
        /** Mapping between a jobID and a list of threads/executors for it **/
	private HashMap<Integer, List<ExecutorService>> jobExecutors;
	
	public DataNode(Communicator communicator) {
		super(communicator);
		mapTasks = Collections.synchronizedList(new ArrayList<MapTask>());
		reduceTasks = Collections.synchronizedList(new ArrayList<ReduceTask>());
		jobExecutors = new HashMap<Integer, List<ExecutorService>>();
		mapLock = new ReentrantLock();
		reduceLock = new ReentrantLock();
		
	}

        /**
         * getMapLoad: Send current number of map tasks running on the node
         * @return
         */
	public int getMapLoad() {
		int size;
		mapLock.lock();
		        size = mapTasks.size();
		mapLock.unlock();
		return (size);
	}

        /**
         * getReduceLoad: send current number of reduce tasks running on the node
         * @return
         */
	public int getReduceLoad() {
		int size;
		reduceLock.lock();
		        size = reduceTasks.size();
		reduceLock.unlock();
		return (size);
	}

        /**
         * performMap:
         * Add the mapTask to the list of current running mapTasks,
         * Allocate a new MapRunner and an executor service and
         * start the mapTask
         * @param mapTask
         */
	public void performMap(MapTask mapTask) {
		mapLock.lock();
		mapTasks.add(mapTask);
		mapLock.unlock();
		
		
		MapRunner runner = new MapRunner(mapTask);
                // add the DataNode as a listener for this task
		runner.addListener(this);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		addExecutorToMap(mapTask.getJobConf().getJobID(), executor);
		executor.execute(runner);
	}

        /**
         * addExecutor:
         * @param jobID: jobID for which the an executor service was created
         * @param executor: executor to be added to the map
         */
	private void addExecutorToMap(int jobID, ExecutorService executor) {
		if(jobExecutors.get(jobID)==null) {
			List<ExecutorService> executors = new ArrayList<ExecutorService>();
			executors.add(executor);
			jobExecutors.put(jobID, executors);
		}
		else {
			jobExecutors.get(jobID).add(executor);
		}
	}

        /**
         * notifyOfMapperThreadCompletion:
         * We come in here when a mapper task is successfully completed.
         * At this point the mapper has generated intermediate files. In this function
         * we run the partition function on the keys and decide which reducers we need to send
         * the intermediate files to.
         *
         * The sending of intermediate files as soon as a map task
         * completes is better than sending all at the end, especially in situations where
         * the network bandwidth can be a bottleneck.
         * @param mapRunner
         */
	@Override
	public synchronized void notifyOfMapperThreadCompletion(MapRunner mapRunner) {
                MapTask finished = mapRunner.getMapTask();
                JobConfiguration jobConf = finished.getJobConf();
                final int chunkID = finished.getChunkID();
		File folder = new File(jobConf.getJobDir() + "-intermediate/");
			
		File [] intermediateFiles = folder.listFiles(new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
		        return name.endsWith(Integer.toString(chunkID));
		    }
		});

                /**
                 * Iterate through all the intermediate files created for a particular
                 * chunkID
                 */
		for(File file : intermediateFiles) {
			try {
                                /**
                                 * The intermediate filename is key-chunkID.
                                 * Extract the key from the filename
                                 */
				String[] arr = file.getName().split("-");
				String key = "";
				
				for(int i = 0; i < arr.length - 1; i++) {
					key += arr[i];
				}

                                //create new partitioner object
				Partitioner partitioner = new Partitioner(jobConf.getNumberOfReducers(), key);
                                /**
                                 * run a partition function which returns a reducer to which this
                                 * intermediate file should be sent to
                                 */
				int dataNodeIndex = partitioner.partition();
                                /**
                                 * get the list of all the data nodes. Sort them.
                                 * Use the dataNodeIndex returned by the partition function
                                 * to get the actual reducer node
                                 */
				communicator.acquireDataNodesLock();
				List<String> allDataNodes = communicator.getAllDataNodes();
				Collections.sort(allDataNodes);
				String reducerNode = allDataNodes.get(dataNodeIndex);

                                /**
                                 * Get the communicator object of the reducer node,
                                 * Read the intermediate file into the memory and call receiveIntermediateFile()
                                 * on the communicator of the reducer node.
                                 */
				String intermediateFilePath = jobConf.getJobDir()+"-intermediate/"
                                        + key.toString() + "-" + chunkID;
				Registry registry;
				registry = LocateRegistry.getRegistry(reducerNode, communicator.getREGISTRY_PORT());
				CommunicatorInterface communicator =
                                        (CommunicatorInterface) registry.lookup("communicator_"+reducerNode);
				communicator.receiveIntermediateFile(jobConf, Files.readAllBytes(Paths.get(intermediateFilePath)),
                                        file.getName());

                                /**
                                 * At the end of the task completion we send to master a list of reducer nodes to
                                 * which intermediate files were sent to. Hence store this reducer node to the list
                                 */
                                finished.addReducer(reducerNode);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (IndexOutOfBoundsException e) {
				System.out.println("This job is about to be terminated");
			}
			communicator.releaseDataNodesLock();
		}

                // notify the master that map task has successfully completed
                notifyMasterOfMapTaskCompletion(finished);

                // remove this task from local data structure
                mapLock.lock();
                mapTasks.remove(finished);
                mapLock.unlock();

	}

        /**
         * notifyMasterOfMapTaskCompletion:
         * Get the master's communicator object and call mapTaskCompleted on the communicator
         * @param finished: the map task that completed
         */
        private void notifyMasterOfMapTaskCompletion(MapTask finished) {
                Registry registry;
                String masterNode = communicator.getMasterHostName();
                JobConfiguration jobConf = finished.getJobConf();

                try {
                        registry = LocateRegistry.getRegistry(masterNode, communicator.getREGISTRY_PORT());
                        CommunicatorInterface communicator =
                                (CommunicatorInterface) registry.lookup("communicator_"+masterNode);
                        communicator.mapTaskCompleted(finished);
                } catch (RemoteException e) {
                        e.printStackTrace();
                } catch (NotBoundException e) {
                        e.printStackTrace();
                }
        }

        /**
         * performReduce:
         * Add the reduceTask to the list of current running reduceTasks,
         * Allocate a new ReduceRunner and an executor service and
         * start the reduceTask
         * @param reduceTask: reduce task to be added
         */

        public void performReduce(ReduceTask reduceTask) {
        	JobConfiguration jobConf = reduceTask.getJobConf();
                Path path = Paths.get(jobConf.getJobDir() + "/" + jobConf.getOutputDirectoryPath());

                // create an output directory for the final output if one doesn't exist
                if(!Files.exists(path)) {
                        File dir = new File(jobConf.getJobDir()+ "/" +jobConf.getOutputDirectoryPath());
                        dir.mkdir();
                }

                reduceLock.lock();
                reduceTasks.add(reduceTask);
                reduceLock.unlock();

                ReduceRunner runner = new ReduceRunner(reduceTask);
                // add the DataNode as a listener for this task
                runner.addListener(this);
                ExecutorService executor = Executors.newSingleThreadExecutor();
                addExecutorToMap(reduceTask.getJobConf().getJobID(), executor);
                executor.execute(runner);
	}

        /**
         * notifyOfReducerThreadCompletion:
         * notify the master and remove the reduce task from local data structure
         * @param reduceRunner
         */
	@Override
	public void notifyOfReducerThreadCompletion(ReduceRunner reduceRunner) {
        ReduceTask finished = reduceRunner.getTask();

        notifyMasterOfReduceTaskCompletion(finished);
        reduceLock.lock();
        reduceTasks.remove(reduceRunner);
        reduceLock.unlock();

	}

        /**
         * notifyMasterOfReduceTaskCompletion:
         * Get the master's communicator and call reduceTaskCompleted on it
         * @param finished: the reduce task that just completed
         */
        private void notifyMasterOfReduceTaskCompletion(ReduceTask finished) {
            Registry registry;
            String masterNode = communicator.getMasterHostName();

            try {
                    registry = LocateRegistry.getRegistry(masterNode, communicator.getREGISTRY_PORT());
                    CommunicatorInterface communicator =
                            (CommunicatorInterface) registry.lookup("communicator_" + masterNode);
                    communicator.reduceTaskCompleted(finished);
            } catch (RemoteException e) {
                    e.printStackTrace();
            } catch (NotBoundException e) {
                    e.printStackTrace();
            }
        }

        /**
         * mapTaskFailureDetected: Retry the failed map task if is has not
         * exceeded it's maximum retry counts. Otherwise bail out and notify
         * master of failure
         * @param task: map task that failed
         * @param e: Exception that caused the task to fail
         */
	public void mapTaskFailureDetected(MapTask task, Exception e) {
		if (task.getRetryCount() == 1) {
			mapLock.lock();
			mapTasks.remove(task);
			mapLock.unlock();
			notifyMasterOfMapTaskFailure(task, e);
		} else {
                        // get the retry count
			int count = task.getRetryCount();
                        // increment it
			count++;
                        // set the incremented retry count
			task.setRetryCount(count);
                        // try to run it again
			MapRunner runner = new MapRunner(task);
			runner.addListener(this);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			addExecutorToMap(task.getJobConf().getJobID(), executor);
			executor.execute(runner);
		}
	}

        /**
         * reduceTaskFailureDetected: Retry the failed reduce task if is has not
         * exceeded it's maximum retry counts. Otherwise bail out and notify
         * master of failure
         * @param task: reduce task that failed
         * @param e: Exception that caused the task to fail
         */
	public void reduceTaskFailureDetected(ReduceTask task, Exception e) {
		if (task.getRetryCount() == 1) {
			reduceLock.lock();
			reduceTasks.remove(task);
			reduceLock.unlock();
			notifyMasterOfReduceTaskFailure(task, e);
		} else {
                        // get the retry count
			int count = task.getRetryCount();
                        // increment it
			count++;
                        // set the incremented retry count
			task.setRetryCount(count);
                        // try to run it again
			ReduceRunner runner = new ReduceRunner(task);
			runner.addListener(this);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			addExecutorToMap(task.getJobConf().getJobID(), executor);
			executor.execute(runner);
		}
	}

        /**
         * notifyMasterOfMapTaskFailure:
         * Get master's communicator and call mapTaskFailed on it with the failed
         * maptask and the exception
         * @param task
         * @param e
         */
	public void notifyMasterOfMapTaskFailure(MapTask task, Exception e) {
                Registry registry;
                String masterNode = communicator.getMasterHostName();

                try {
                        registry = LocateRegistry.getRegistry(masterNode, communicator.getREGISTRY_PORT());
                        CommunicatorInterface communicator =
                                (CommunicatorInterface) registry.lookup("communicator_" + masterNode);
                        communicator.mapTaskFailed(task, e);
                } catch (RemoteException e1) {
                            e1.printStackTrace();
                } catch (NotBoundException e1) {
                            e1.printStackTrace();
                }
	}

        /**
         * notifyMasterOfReduceTaskFailure:
         * Get master's communicator and call reduceTaskFailed on it with the failed
         * reduceTask and the exception
         * @param task
         * @param e
         */
        public void notifyMasterOfReduceTaskFailure(ReduceTask task, Exception e) {
                Registry registry;
                String masterNode = communicator.getMasterHostName();

                try {
                        registry = LocateRegistry.getRegistry(masterNode, communicator.getREGISTRY_PORT());
                        CommunicatorInterface communicator =
                                (CommunicatorInterface) registry.lookup("communicator_" + masterNode);
                        communicator.reduceTaskFailed(task, e);
                } catch (RemoteException e1) {
                        e1.printStackTrace();
                } catch (NotBoundException e1) {
                        e1.printStackTrace();
                }
	}

        /**
         * abortJob: abort on going tasks for this job
         * @param jobID
         */
	public void abortJob(int jobID) {
                // get a list of executors for this job
		List<ExecutorService> executors = jobExecutors.get(jobID);
		System.out.println(communicator.getLocalHostName() + " proceeding to abort job "+jobID );
		if(executors!=null) {
			for(ExecutorService executor : executors) {
				System.out.println(communicator.getLocalHostName() +
                                        " is shutting down executor "+executor.toString() );
                                //shutdown the executor
				executor.shutdown();
			}
                        //cleanup
			jobExecutors.remove(jobID);
			removeJobTasks(jobID);
		}
	}

        /**
         * removeJobTasks: Cleanup mapTasks and reduceTasks for this job
         * @param jobID
         */
	private void removeJobTasks(int jobID) {
		System.out.println(communicator.getLocalHostName() + " proceeding to remove map tasks for job "+jobID );
		mapLock.lock();
			Iterator<MapTask> itr = mapTasks.iterator();
			while(itr.hasNext()) {
				MapTask task = itr.next();
				if(task.getJobConf().getJobID()==jobID) {
					itr.remove();
				}
			}
		mapLock.unlock();

		System.out.println(communicator.getLocalHostName()
                        + " proceeding to remove reduce tasks for job "+jobID );
		reduceLock.lock();
			Iterator<ReduceTask> itr1 = reduceTasks.iterator();
			while(itr1.hasNext()) {
				ReduceTask task = itr1.next();
				if(task.getJobConf().getJobID()==jobID) {
					itr1.remove();
				}
			}
		reduceLock.unlock();
		
	}

        /**
         * fsCleanup: Cleanup intermediate files and reducer files directory for a job
         * @param jobConf: job's configuration
         */
	public void fsCleanup(JobConfiguration jobConf) {
		Path path = Paths.get(jobConf.getJobDir()+ "-intermediate");
		
		if(Files.exists(path)) {
			File dir = new File(jobConf.getJobDir()+ "-intermediate");
			deleteDir(dir);
		}
		
		path = Paths.get(jobConf.getJobDir()+ "-reducerfiles");
		if (Files.exists(path)) {
			File dir = new File(jobConf.getJobDir()+ "-reducerfiles");
			deleteDir(dir);
		}
	}

        /**
         * deleteDir: worker function to delete a directory
         * @param dir
         */
	private void deleteDir(File dir) {
		if (dir.listFiles() == null) {
			return;
		}
		
		for (File f : dir.listFiles()) {
			f.delete();
		}
		
		dir.delete();
	}
}
