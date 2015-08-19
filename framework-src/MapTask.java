/**
 * Class MapTask: This class contains fields and methods related to a single instance of a map task
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MapTask implements Serializable {

        // job configuration
	private JobConfiguration jobConf;
        // chunk ID on which this map task will be performed
	private int chunkID;
        // node on which this map task is running
	private String runningNode;

        // list of reducers this maptask sends intermediate files to
        private List<String> reducers;
        // how many times has this map task been retried after failing
        private int retryCount= 0;
	
	public MapTask(JobConfiguration jobConf, int chunkID, String runningNode) {
		this.setJobConf(jobConf);
		this.setChunkID(chunkID);
		reducers = new ArrayList<String>();
		setRunningNode(runningNode);
	}

	public JobConfiguration getJobConf() {
		return jobConf;
	}

	public void setJobConf(JobConfiguration jobConf) {
		this.jobConf = jobConf;
	}

	public int getChunkID() {
		return chunkID;
	}

	public void setChunkID(int chunkID) {
		this.chunkID = chunkID;
	}

        public void addReducer(String reducer) {
            reducers.add(reducer);
        }

        public List<String> getReducers() {
        return reducers;
        }

        /**
         * equals()
         * We are sending over the map task from master to data nodes and back from data nodes to the master node.
         * Each of them keeps track of the map tasks at their ends. In order to correctly maintain the list of tasks at
         * both the ends we override the equals function for the map tasks
         * Equality for a map task is determined by jobID, chunkID and the runningNode
         * @param o
         * @return
         */
        @Override
        public boolean equals(Object o) {
        boolean isEqual = false;

        if(!(o instanceof MapTask)) {
                return false;
        }
        else {
                MapTask task = (MapTask) o;

                if((jobConf.getJobID()==task.getJobConf().getJobID()) && chunkID==task.getChunkID()
                        && runningNode.equals(task.getRunningNode()))
                {
                        isEqual = true;
                }
        }
        return isEqual;
        }

        /**
         * hashCode: Need to override the hashCode method because we override equals
         * @return
         */
        @Override
        public int hashCode() {
        int result = 17;
        result += 31 * chunkID;
        result += 31 * jobConf.getJobID();
        result += 31 * runningNode.hashCode();
        return result;
        }

        public String getRunningNode() {
                return runningNode;
        }

        public void setRunningNode(String runningNode) {
                this.runningNode = runningNode;
        }

        public int getRetryCount() {
                return retryCount;
        }

        public void setRetryCount(int retryCount) {
                this.retryCount = retryCount;
        }
}
