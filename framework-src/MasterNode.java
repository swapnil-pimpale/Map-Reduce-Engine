/**
 * Class MasterNode:
 * This class defines and implements methods run by the master node / coordinator
 * of the framework.
 *
 * The master node is responsible for the following:
 * - Accept request from the client to start jobs
 * - Split the input files into small chunks and distribute them
 * as evenly as possible between all the available DataNodes
 * - Maintain a replication factor for all the chunks in HDFS
 * - Maintain metadata of the chunk locations in the namespace
 * - Schedule map and reduce tasks based on the current loads on the nodes
 * - Monitor health of all the DataNodes using heart beat mechanism
 * - Handle individual task failures
 * - Handle node failures
 * - Cleanup after the jobs are completed
 */
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;


public class MasterNode extends Node {

        // unique job ID
        private int jobIDCounter = 0;
        // unique chunk ID
        private int currentChunkID = 0;
        // namespace mapping jobs to chunks and chunks to nodes
        private NameSpace namespace;
        // mapping from jobID to mapper tasks
        private HashMap<Integer, List<MapTask>> mapperTasks;
        // mapping from jobID to reducer tasks
        private HashMap<Integer, List<ReduceTask>> reducerTasks;
        // mapping from jobID to a list of reducers where reduction will be performed
        private HashMap<Integer, HashSet<String>> jobReducers;
        private HashMap<Integer, Integer> reduceTaskCounter;

        // mapping from node to map tasks running on it
        private HashMap<String, List<MapTask>> nodeMapTasks;
        // mapping from node to reduce tasks running on it
        private HashMap<String, List<ReduceTask>> nodeReduceTasks;
        // are all the maps scheduled for this jobID?
        private HashMap<Integer, Boolean> allMapsScheduled;
        // is this job aborted?
        private HashMap<Integer, Boolean> isAborted;
        // a list of job configuration of currently active jobs
        private List<JobConfiguration> jobConfList;
	
	public MasterNode(Communicator communicator) {
		super(communicator);
		namespace = new NameSpace();
		mapperTasks = new HashMap<Integer, List<MapTask>>();
		jobReducers = new HashMap<Integer, HashSet<String>>();
		allMapsScheduled = new HashMap<Integer, Boolean>();
		reducerTasks = new HashMap<Integer, List<ReduceTask>>();
		reduceTaskCounter = new HashMap<Integer, Integer>();
		nodeMapTasks = new HashMap<String, List<MapTask>>();
		nodeReduceTasks = new HashMap<String, List<ReduceTask>>();
		isAborted = new HashMap<Integer, Boolean>();
		jobConfList = new ArrayList<JobConfiguration>();
	}

        /**
         * assignJobID: assign a unique job ID to every new job (even the restarted job)
         * @param jobconf
         */
	public void assignJobID(JobConfiguration jobconf) {
		jobIDCounter++;
		jobconf.setJobID(jobIDCounter);
	}

        /**
         * splitInputFile: read an input file one record at a time.
         * Create chunks one by one and send them to DataNodes with replication.
         * @param jobconf
         * @param inputFilePath
         * @return
         */
	public NameSpace splitInputFile(JobConfiguration jobconf, String inputFilePath) {
		String nextRecord;
                // get number of records per chunk
		int recPerChunk = communicator.getNumRecPerChunk();
		int currRec = 0;
		Chunk chunk = new Chunk(assignChunkID());
		List<Chunk> chunkList = new ArrayList<Chunk>();

                // read one record at a time
		RecordReader rr = new RecordReader(inputFilePath);
		while ((nextRecord = rr.getNextRecord()) != null) {
			if (currRec < recPerChunk-1) {
                                // add record to the chunk
				chunk.addRecordToChunk(nextRecord);
				currRec++;
			} else {
				//add chunk to list of chunks
				chunk.addRecordToChunk(nextRecord);	
				chunkList.add(chunk);			
				//send chunk at this point
				communicator.sendChunkWithReplication(jobconf, chunk, namespace);
                                // we don't need the chunk data in memory now, so remove it
				chunk.removeChunkData();
                                // start working on a new chunk
				chunk = new Chunk(assignChunkID());		
				currRec = 0;
			}	
		}
                // same steps for the last chunk
		chunkList.add(chunk);
		communicator.sendChunkWithReplication(jobconf, chunk, namespace);
		chunk.removeChunkData();
		rr.close();

                // add the chunk list to the namespace
		namespace.addChunkListToMap1(jobconf.getJobID(), chunkList);
		return namespace;		
	}

        /**
         * assignChunkID: assign unique chunk ID
         * @return
         */
	public int assignChunkID() {
		return ++currentChunkID;
	}

        /**
         * getNameSpace: return master's namespace object
         * @return
         */
	public NameSpace getNameSpace() {
		return namespace;
	}

        /**
         * scheduleJob: The scheduling logic works as follows:
         * - Get the jobID
         * - Get chunk list associated with this jobID
         * - iterate through the chunk list determining which nodes have this chunk
         * - check the loads of these nodes
         * - if the loads are not MAX the send map request and move on to the next chunk
         * - if all the nodes where this chunk is located are running at max load then choose a "foreign" node
         * and send this chunk to the foreign node and assign the map task to it.
         * @param jobconf
         */
	public void scheduleJob(JobConfiguration jobconf) {
                // get chunk list for this job
		List<Chunk> chunkList = namespace.getChunkListFromMap1(jobconf.getJobID());
		Registry registry;
		CommunicatorInterface dataNodeCommunicator;

		allMapsScheduled.put(jobconf.getJobID(), false);
		isAborted.put(jobconf.getJobID(), false);
		int chunkIndex = 0;
                // iterate through the chunk list and also check that the job is not aborted in the meanwhile
		while(chunkIndex < chunkList.size() && !isAborted.get(jobconf.getJobID())) {
			Chunk chunk = chunkList.get(chunkIndex);
                        // get the local nodes for this chunk
			List<String> chunkLocalNodes = namespace.getNodeListFromMap2(chunk.getChunkID());

                        // determine the least busy node from the local nodes
			int minLoad = communicator.getMAXIMUM_MAPS();
			String leastBusyNode = chunkLocalNodes.get(0);
			
			for(String node : chunkLocalNodes) {
				try {
					registry = LocateRegistry.getRegistry(node, communicator.getREGISTRY_PORT());
					dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + node);
					int nodeLoad = dataNodeCommunicator.getMapLoad();
					
					if(nodeLoad < minLoad) {
						minLoad = nodeLoad;
						leastBusyNode = node;
					}					
				} catch (RemoteException e) {
					//e.printStackTrace();
				} catch (NotBoundException e) {
					//e.printStackTrace();
				}
				
			}
			
			if (minLoad == communicator.getMAXIMUM_MAPS()) {
				//for this chunk, all it's 'local' nodes are at full xapacity
				//check all other nodes to see if any is free, and send the chunk over
				//if all nodes are at max, repeat the outer for loop with the same chunk
				communicator.acquireDataNodesLock();
				List<String> foreignNodes = new ArrayList<String>(communicator.getAllDataNodes());
				communicator.releaseDataNodesLock();
				foreignNodes.removeAll(chunkLocalNodes);
				
				for(String toNode : foreignNodes) {
					try {
						CommunicatorInterface fromCommunicator, toCommunicator;
						registry = LocateRegistry.getRegistry(toNode, communicator.getREGISTRY_PORT());
						toCommunicator = (CommunicatorInterface) registry.lookup("communicator_" +
                                                                  toNode);
						int nodeLoad = toCommunicator.getMapLoad();
						
						if(nodeLoad < communicator.getMAXIMUM_MAPS()) {
                                                        /**
                                                         * fromNode = source node
                                                         * toNode = destination node
                                                         * grab chunk from fromNode and send it to toNode
                                                         * perform necessary bookkeeping
                                                         */
							String fromNode = chunkLocalNodes.get(0);
							registry = LocateRegistry.getRegistry(fromNode,
                                                                communicator.getREGISTRY_PORT());
							fromCommunicator = (CommunicatorInterface) registry.lookup("communicator_"
                                                                + fromNode);
							Chunk grabbedChunk = fromCommunicator.grabChunk(jobconf,
                                                                                                        chunk.getChunkID());
							toCommunicator.receiveChunk(jobconf, grabbedChunk);
							namespace.addToMap2(chunk.getChunkID(), toNode);
							MapTask mapTask = new MapTask(jobconf, chunk.getChunkID(), toNode);
							addMapperTask(jobconf.getJobID(), mapTask);
							addToNodeMapTasks(toNode, mapTask);
							toCommunicator.performMap(mapTask);
							chunkIndex++;
							break;
						}						
					} catch (RemoteException e) {
						//e.printStackTrace();
					} catch (NotBoundException e) {
						//e.printStackTrace();
					}					
				}
			} else {
				//send map request to node, and update its loads/tasks
				try {
					registry = LocateRegistry.getRegistry(leastBusyNode, communicator.getREGISTRY_PORT());
					dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + leastBusyNode);
					MapTask mapTask = new MapTask(jobconf, chunk.getChunkID(), leastBusyNode);
					addMapperTask(jobconf.getJobID(), mapTask);
					addToNodeMapTasks(leastBusyNode, mapTask);
					dataNodeCommunicator.performMap(mapTask);		
					chunkIndex++;
				} catch (RemoteException e) {
					//e.printStackTrace();
				} catch (NotBoundException e) {
					//e.printStackTrace();
				}
				
			}
			
		}
		
		allMapsScheduled.put(jobconf.getJobID(), true);
		
	}

        /**
         * addToNodeMapTasks: make a note that mapTask is running on node
         * @param node
         * @param mapTask
         */
	private void addToNodeMapTasks(String node, MapTask mapTask) {
		if (nodeMapTasks.get(node) == null) {
			List<MapTask> list = new ArrayList<MapTask>();
			list.add(mapTask);
			nodeMapTasks.put(node, list);
		} else {
			nodeMapTasks.get(node).add(mapTask);
		}
	}

        /**
         * addToNodeReduceTasks: make a note that reduceTask is running on node
         * @param node
         * @param reduceTask
         */
	private void addToNodeReduceTasks(String node, ReduceTask reduceTask) {
		if (nodeReduceTasks.get(node) == null) {
			List<ReduceTask> list = new ArrayList<ReduceTask>();
			list.add(reduceTask);
			nodeReduceTasks.put(node, list);
		} else {
			nodeReduceTasks.get(node).add(reduceTask);
		}
	}

        /**
         * addMapperTask: add mapTask to mapperTasks hashmap
         * @param jobID
         * @param mapTask
         */
	private void addMapperTask(int jobID, MapTask mapTask) {
	        if (mapperTasks.get(jobID) == null) {
	                List<MapTask> list = new ArrayList<MapTask>();
	                list.add(mapTask);
	                mapperTasks.put(jobID, list);
	        } else {
	                mapperTasks.get(jobID).add(mapTask);
	        }
	}

        /**
         * removeMapperTask: We come in here if a map task has completed. At this point
         * we have the opportunity to check whether we can start the reduce phase for this
         * job.
         * We start the reduce phase only when the map phase is complete.
         * @param jobID
         * @param mapTask
         */
	public void removeMapperTask(int jobID, MapTask mapTask) {
		//check if all mapTasks removed. If yes, schedule Reduce jobs
                if (jobReducers.get(jobID) == null) {
                        HashSet<String> hashSet = new HashSet<String>(mapTask.getReducers());
                        jobReducers.put(jobID, hashSet);
                } else {
                        Iterator<String> itr = mapTask.getReducers().iterator();
                        while (itr.hasNext()) {
                                jobReducers.get(jobID).add(itr.next());
                        }
                }

                if (mapperTasks.get(jobID) != null) {
                        // remove this map task from the hasmaps
                        mapperTasks.get(jobID).remove(mapTask);
                        nodeMapTasks.get(mapTask.getRunningNode()).remove(mapTask);

                        if (mapperTasks.get(jobID).size() == 0 && allMapsScheduled.get(jobID)) {
                                scheduleReduce(mapTask.getJobConf());
                        }
                }
	}

        /**
         * scheduleReduce:
         * Get a list of reducers to which intermediate sorted files were sent to,
         * Iterate through the list and sent reduce request to all of them
         * @param jobConf: job's configuration
         */
	private void scheduleReduce(JobConfiguration jobConf) {
		int jobID = jobConf.getJobID();
                //get a set of reducers
		HashSet<String> reducerSet = jobReducers.get(jobID);
		Iterator<String> itr = reducerSet.iterator();
		Registry registry;
		CommunicatorInterface reduceCommunicator;
		
		while (itr.hasNext()) {
			String reducer = itr.next();
			
			try {
                                // get the communicator object for each reducer
				registry = LocateRegistry.getRegistry(reducer, communicator.getREGISTRY_PORT());
				reduceCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + reducer);
                                // create a new ReduceTask
				ReduceTask reduceTask = new ReduceTask(jobConf, reducer);
				addReducerTask(jobConf.getJobID(), reduceTask);
				addToNodeReduceTasks(reducer, reduceTask);
                                // call performReduce() on the reducer's communicator
				reduceCommunicator.performReduce(reduceTask);				
				} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				//e.printStackTrace();
			}			
		}
		
	}

        /**
         * addReducerTask: add reduceTask to a local hashmap
         * @param jobID
         * @param reduceTask
         */
	private void addReducerTask(int jobID, ReduceTask reduceTask) {
		if (reducerTasks.get(jobID) == null) {
			List<ReduceTask> list = new ArrayList<ReduceTask>();
			list.add(reduceTask);
			reducerTasks.put(jobID, list);
		} else {
			reducerTasks.get(jobID).add(reduceTask);
		}
	}

        /**
         * removeReducerTask: We come in here if a reducer task has successfully completed.
         * Check if all reduce tasks are completed. If yes, cleanup the job
         * @param reduceTask
         */
	public void removeReducerTask(ReduceTask reduceTask) {
		int jobID = reduceTask.getJobConf().getJobID();
		JobConfiguration jobConf = reduceTask.getJobConf();

                // update completed reduce tasks
		if (reduceTaskCounter.get(jobID) == null) {
			reduceTaskCounter.put(jobID, 1);
		} else {
			int curr_completed = reduceTaskCounter.get(jobID);
			curr_completed++;
			reduceTaskCounter.put(jobID, curr_completed);
		}

                // remove reduce task from the local hashmap
		reducerTasks.get(jobID).remove(reduceTask);
		String runningNode = reduceTask.getRunningNode();
		System.out.println("Node "+runningNode+" has successfully completed it's reduce task for job name: "+
		reduceTask.getJobConf().getJobName() + " and job ID: " +jobID);
		nodeReduceTasks.get(runningNode).remove(reduceTask);

                // if all reduce tasks completed, cleanup!!!
		if (reduceTaskCounter.get(jobID) == jobConf.getNumberOfReducers()) {
			System.out.println("Job name: "+reduceTask.getJobConf().getJobName() + " with job ID: " +jobID+
					" has successfully completed.");
			jobCleanUp(jobConf);
		}
		
	}

        /**
         * jobCleanUp: Cleanup the in-memory footprint of the job and notify all the dataNodes of the same
         * @param jobConfiguration
         */
	public void jobCleanUp(JobConfiguration jobConfiguration) {
                int jobID = jobConfiguration.getJobID();

                mapperTasks.remove(jobID);
                reducerTasks.remove(jobID);
                jobReducers.remove(jobID);
                reduceTaskCounter.remove(jobID);
                allMapsScheduled.remove(jobID);
                namespace.removeJobFromMaps(jobID);
                reduceTaskCounter.remove(jobID);
                removeJobTasks(jobID);
                notifyNodesofFSCleanup(jobConfiguration);
	}

        /**
         * removeJobTasks:
         * As part of the cleanup, remove map and reduce tasks from nodeMapTasks
         * and nodeReduceTasks respectively.
         * @param jobID: jobID for which the tasks are to be removed
         */
	private void removeJobTasks(int jobID) {
                Collection<List<MapTask>> mapTasks = nodeMapTasks.values();
                Iterator<List<MapTask>> mapItr1 = mapTasks.iterator();

                while(mapItr1.hasNext()) {
                        List<MapTask> tasks = mapItr1.next();
                        Iterator<MapTask> mapItr2 = tasks.iterator();
                        while(mapItr2.hasNext()) {
                                MapTask task = mapItr2.next();
                                if(task.getJobConf().getJobID()==jobID) {
                                        mapItr2.remove();
                                }
                        }
                }

                Collection<List<ReduceTask>> reduceTasks = nodeReduceTasks.values();
                Iterator<List<ReduceTask>> reduceItr1 = reduceTasks.iterator();

                while(reduceItr1.hasNext()) {
                        List<ReduceTask> tasks = reduceItr1.next();
                        Iterator<ReduceTask> reduceItr2 = tasks.iterator();
                        while(reduceItr2.hasNext()) {
                                ReduceTask task = reduceItr2.next();
                                if(task.getJobConf().getJobID()==jobID) {
                                        reduceItr2.remove();
                                }
                        }
                }

                Iterator<JobConfiguration> jobItr = jobConfList.iterator();
                while(jobItr.hasNext()) {
                        JobConfiguration jobconf = jobItr.next();
                        if(jobconf.getJobID()==jobID) {
                                jobItr.remove();
                        }
                }
	}

        /**
         * startHeartBeatThread:
         * Start a separate thread to monitor status of all the DataNodes in the system
         */
	public void startHeartBeatThread() {
		HeartBeatThread hbt = new HeartBeatThread(communicator.getMasterHostName(),
                                        communicator.getAllDataNodes(), communicator.getREGISTRY_PORT(), this);
		new Thread(hbt).start();
	}

        /**
         * mapTaskFailed: We come in here if a mapper reported failure after retrying the task.
         * In this case, the master aborts the job.
         * @param task
         * @param e
         */
	public void mapTaskFailed(MapTask task, Exception e) {
		int id = task.getJobConf().getJobID();

                // if not already aborted, abort it now
		if (allMapsScheduled.get(id) && !isAborted.get(id)) {
			isAborted.put(id, true);
			System.out.println("ABORTING JOB");
			abortJob(task.getJobConf());
		}
	}

        /**
         * abortJob: Notify nodes to stop doing whatever they are doing for this job
         * and then cleanup this job
         * @param jobConfiguration
         */
	private void abortJob(JobConfiguration jobConfiguration) {
		notifyNodesOfJobFailure(jobConfiguration.getJobID());		
		jobCleanUp(jobConfiguration);
		System.out.println("JOB: "+jobConfiguration +" cleaned up." );
	}

        /**
         * notifyNodesofFSCleanup:
         * Get communicator for all the dataNodes and call fsCleanup() on each of them
         * @param jobConfiguration
         */
	private void notifyNodesofFSCleanup(JobConfiguration jobConfiguration) {
		communicator.acquireDataNodesLock();
		List<String> allNodes = communicator.getAllDataNodes();
		Registry registry;
		CommunicatorInterface dataNodeCommunicator;
		
		for(String node : allNodes) {
			try {
				registry = LocateRegistry.getRegistry(node, communicator.getREGISTRY_PORT());
				dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_"+node);
				dataNodeCommunicator.fsCleanup(jobConfiguration);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				//e.printStackTrace();
			}
		}
		communicator.releaseDataNodesLock();
	}

        /**
         * notifyNodesOfJobFailure:
         * Get communicators for each of the dataNodes and call abortJob() on them
         * @param jobID
         */
	private void notifyNodesOfJobFailure(int jobID) {
		communicator.acquireDataNodesLock();
                //get a list of dataNodes
		List<String> allNodes = communicator.getAllDataNodes();
		Registry registry;
		CommunicatorInterface dataNodeCommunicator;
		
		for(String node : allNodes) {
			try {
				registry = LocateRegistry.getRegistry(node, communicator.getREGISTRY_PORT());
				dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + node);
				System.out.println("Sending abort request to node: " + node);
				dataNodeCommunicator.abortJob(jobID);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				//e.printStackTrace();
			}
		}
		communicator.releaseDataNodesLock();
	}

        /**
         * reduceTaskFailed: We come in here if a reducer reported failure after retrying the reduce task.
         * In this case, the master aborts the job.
         * @param task
         * @param e
         */
	public void reduceTaskFailed(ReduceTask task, Exception e) {
		int id = task.getJobConf().getJobID();
		System.out.println("REDUCETASK FOR JOBID " + task.getJobConf().getJobID() + " FAILED");
                // if not already aborted, abort it
		if (!isAborted.get(id)) {
			isAborted.put(id, true);
			abortJob(task.getJobConf());
		}
	}

        /**
         * storeJob: store job configuration locally
         * Need it for restarting the job
         * @param jobconf
         */
	public void storeJob(JobConfiguration jobconf) {
		jobConfList.add(jobconf);
	}

        /**
         * failureDetected: We come in here if the heartbeat thread detects a node failure
         * The following steps are carried out in this case:
         * - remove this failed node from the local list of all the nodes
         * - get a list of jobs that were running on the failed node
         * - send abortJob notifications to all participants for these jobs
         * - resubmit/restart these jobs all over again
         *
         *
         * @param failedNode
         */
	public void failureDetected(String failedNode) {
                //remove the failedNode from DataNodeList
		communicator.getDataNodeList().remove(failedNode);
		communicator.acquireDataNodesLock();
                // get a list of all remaining DataNodes
		List<String> allNodes = communicator.getAllDataNodes();
		Registry registry;
		CommunicatorInterface dataNodeCommunicator;
		
		for(String node : allNodes) {
			try {
				registry = LocateRegistry.getRegistry(node, communicator.getREGISTRY_PORT());
				dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + node);
                                // inform all dataNodes to remove the failed node from their list
				dataNodeCommunicator.removeFailedNode(failedNode);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				//e.printStackTrace();
			}
		}
		communicator.releaseDataNodesLock();

                // construct a list of jobs to be restarted
		HashSet<Integer> jobsToBeRestarted = getJobsToBeRestarted(failedNode);
		Iterator<Integer> jobItr = jobsToBeRestarted.iterator();
		List<JobConfiguration> restartJobConfs = new ArrayList<JobConfiguration>();
		
		while(jobItr.hasNext()) {
			int jobID = jobItr.next();
                        // abort all jobs in the list
			
			 try {
				 Iterator<JobConfiguration> jobConfItr = jobConfList.iterator();
	             while(jobConfItr.hasNext()) {
	            	 JobConfiguration jobconf = jobConfItr.next();
	            	 if(jobconf.getJobID()==jobID) {
	 					//Shouldn't enter more than once
	 					restartJobConfs.add(jobconf);
	 					System.out.println("Job "+jobconf.getJobID()+ " has failed needs to be restarted.");
	 					isAborted.put(jobID, true);
	 					abortJob(jobconf);
	 				}
	             }
			 }
			 catch(ConcurrentModificationException e) {
				 
			 }
             
			/*for(JobConfiguration jobconf : jobConfList) {
				if(jobconf.getJobID()==jobID) {
					//Shouldn't enter more than once
					restartJobConfs.add(jobconf);
					System.out.println("Job "+jobconf.getJobID()+ " has failed needs to be restarted.");
					isAborted.put(jobID, true);
					abortJob(jobconf);
				}
			}*/
		}
		
		Iterator<JobConfiguration> jobConfItr = restartJobConfs.iterator();

                // resubmit all jobs that were running on the failed node
		while(jobConfItr.hasNext()) {
			JobConfiguration jobconf = jobConfItr.next();
			System.out.println("Resubmitting/Restarting job " +jobconf.getJobID());
			communicator.submitJob(jobconf);
		}
	}

        /**
         * getJobsToBeRestarted:
         * @param failedNode
         * @return returns a HashSet of the jobs that were running on the faileNode
         * These are all the candidates for restarting
         */
	private HashSet<Integer> getJobsToBeRestarted(String failedNode) {
		HashSet<Integer> jobsToBeRestarted = new HashSet<Integer>();
		List<MapTask> mapTasks = nodeMapTasks.get(failedNode);
		List<ReduceTask> reduceTasks =  nodeReduceTasks.get(failedNode);
		
		if (mapTasks != null) {
			for(MapTask task : mapTasks) {
				jobsToBeRestarted.add(task.getJobConf().getJobID());
			}
		}
		
		if (reduceTasks != null) {
			for(ReduceTask task : reduceTasks) {
				jobsToBeRestarted.add(task.getJobConf().getJobID());
			}
		}
		return jobsToBeRestarted;
	}
}