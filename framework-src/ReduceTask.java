/**
 * Class ReduceTask: This class contains fields and methods related to a single instance of a reduce task
 */
import java.io.Serializable;


public class ReduceTask implements Serializable{

        // job configuration
	private JobConfiguration jobConf;
        // node on which this reduce task is running
	private String runningNode;
        // how many times has this map task been retried after failing
	private int retryCount = 0;
	
	public ReduceTask(JobConfiguration jobConf, String runningNode) {
		this.setJobConf(jobConf);
		setRunningNode(runningNode);
	}

	public JobConfiguration getJobConf() {
		return jobConf;
	}

	public void setJobConf(JobConfiguration jobConf) {
		this.jobConf = jobConf;
	}

	public String getRunningNode() {
		return runningNode;
	}

	public void setRunningNode(String runningNode) {
		this.runningNode = runningNode;
	}

        /**
         * equals()
         * We are sending over the reduce task from master to data nodes and back from data nodes to the master node.
         * Each of them keeps track of the reduce tasks at their ends. In order to correctly maintain the list of tasks at
         * both the ends we override the equals function for the map tasks
         * Equality for a reduce task is determined by jobID and the runningNode
         * @param o
         * @return
         */
        @Override
        public boolean equals(Object o) {
        boolean isEqual = false;

        if(!(o instanceof ReduceTask)) {
                return false;
        }
        else {
                ReduceTask task = (ReduceTask) o;

                if((jobConf.getJobID()==task.getJobConf().getJobID()) && runningNode.equals(task.getRunningNode()))
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
        result += 31 * jobConf.getJobID();
        result += 31 * runningNode.hashCode();
        return result;
        }

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

}
