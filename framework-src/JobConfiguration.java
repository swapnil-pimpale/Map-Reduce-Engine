/**
 * Class JobConfiguration:
 * Contains metadata about the job and corresponding getter and setter methods
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class JobConfiguration implements Serializable{
        // client JAR file location
	private String jarFilePath;
        // location where client input files are stored
	private String inputDirectoryPath;
        // HDFS location where client wants the output files to be stored
	private String outputDirectoryPath;
        // byte data of JAR
	private byte[] jarData;
        // list of input file descriptors
	private List<File> inputFileNames;
        // jobID of this job
	private int jobID;
        // name of this job
	private String jobName;
        // HDFS directory created by us for this job
	private String jobDir;
        // number of reducers client has specified for this job
	private int numberOfReducers;
	
	public JobConfiguration(String jobName, String jarFilePath, String inputDirectoryPath,
			String outputDirectoryPath, int numberOfReducers) {
		this.setJobName(jobName);
		this.jarFilePath = jarFilePath;
		this.inputDirectoryPath = inputDirectoryPath;
		this.setOutputDirectoryPath(outputDirectoryPath);	
		inputFileNames = new ArrayList<File>();
		this.setNumberOfReducers(numberOfReducers);
		jobID = -1;
		readFiles();	
	}

        /**
         * readFiles: read the client JAR file and store it
         * Also store file objects for all the input files
         */
	private void readFiles() {
		try {
			jarData = Files.readAllBytes(Paths.get(jarFilePath));

			File folder = new File(inputDirectoryPath);
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles) {
			    if (file.isFile()) {
			    	inputFileNames.add(file);
			    }
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

        // getter and setters for the above fields

	public byte[] getJarData() {
		return jarData;
	}
	
	public List<File> getinputFileDescriptors() {
		return inputFileNames;
	}
	
	public int getJobID() {
		return jobID;
	}
	
	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setJobDir(String jobDir) {
		this.jobDir = jobDir;
	}
	
	public String getJobDir() {
		return jobDir;
	}

	public int getNumberOfReducers() {
		return numberOfReducers;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}

	public String getOutputDirectoryPath() {
		return outputDirectoryPath;
	}

	public void setOutputDirectoryPath(String outputDirectoryPath) {
		this.outputDirectoryPath = outputDirectoryPath;
	}

}
