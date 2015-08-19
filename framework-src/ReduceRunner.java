import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarFile;

/**
 * The ReduceRunner is a thread that reduces all intermediate key-value pairs for a particular job that are
 * locally present on some node, and writes the reduced output to a single final output file.
 * 
 * The client's reduce method should be of the form: reduce(<MapReduce IO Class> key, 
 * Iterator<MapReduce IO Class> values, OutputCollector<Any MapReduce IO Class, Any MapReduce IO Class> output).
 * This is explained in greater detail in the report.
 * @author Romit
 *
 */
public class ReduceRunner extends TaskRunner {

	private ReduceTask task;
	private DataNode listener;
	private Hashtable table;
	
	public ReduceRunner(ReduceTask task) {
		this.setTask(task);
		table = new Hashtable();
	}

	@Override
	public void run() {
		JobConfiguration jobConf = task.getJobConf();
		
		try {
			//Read the main class from the jar file, and get the class
			JarFile jarFile = new JarFile(jobConf.getJobDir()+"/client.jar");
			String className = jarFile.getManifest().getMainAttributes().getValue("Main-Class");		
			File file  = new File(jobConf.getJobDir()+"/client.jar");
			URL url = file.toURI().toURL(); 
			URL[] urls = new URL[]{url};
			ClassLoader cl = new URLClassLoader(urls);
			Class c = cl.loadClass(className);

			//Construct the object of the main class, get the reduce method for the object
            Object obj = c.newInstance();
            Method[] methods = c.getDeclaredMethods();
            Method reduceMethod = getReduceMethod(methods);         

            Class param0 = reduceMethod.getParameterTypes()[0]; //key for any MapReduce IO class
            Class param1 = reduceMethod.getParameterTypes()[1]; //Iterator over any 'framework-defined' class
            Class param2 = reduceMethod.getParameterTypes()[2]; //OutputCollector
            Method param0setMethod = getSetMethod(param0.getDeclaredMethods()); //set method for the key
            Object parameter0 = param0.newInstance(); //MapReduce IO Object that wraps the intermediate key
            
            String paramType = (((ParameterizedType) reduceMethod.getGenericParameterTypes()[1]).getActualTypeArguments()[0]).toString();
            paramType = paramType.replace("class ", "");
            //paramType is now the 'framework-defined' class, like IntWritable or Text etc.
            
            Class parameterizedType = Class.forName(paramType);
            
            File folder = new File(jobConf.getJobDir()+"-reducerfiles/");
    		File[] intermediateFiles = folder.listFiles();
  
    		/**
    		 * For every intermediate file present on the node, get the intermediate key, and wrap it with
    		 * the appropriate MapReduceIO Object. Read the file for this intermediate key,and for every line in 
    		 * this file, grab the intermediate value(tab separated from the intermediate key)
    		 * and add it to an ArrayList. Invoke the reduce method using the MapReduceIO Object that wraps the key,
    		 * an iterator over the ArrayList of intermediate values, and an OutputCollector object to collect
    		 * the final key-value pairs.
    		 */
    		if(intermediateFiles!=null) {

    			for(File reduceFile : intermediateFiles) {
        			//System.out.println("Reducing file: " + reduceFile.getName());
        			Object collector = param2.newInstance();
    				String[] arr = reduceFile.getName().split("-");
        			String key = "";
        			
        			for(int i=0;i<arr.length-1;i++) {
        				key += arr[i];
        			}

        			RecordReader rr = new RecordReader(jobConf.getJobDir()+"-reducerfiles/"+reduceFile.getName());
    				param0setMethod.invoke(parameter0, key);
    				List values = new ArrayList();
    				String nextRecord;

					while( (nextRecord=rr.getNextRecord())!=null) {
						
						arr = nextRecord.split("\t");	
						Object parameterizedObject = parameterizedType.newInstance();
						Method parameterizedSetMethod = getSetMethod(parameterizedType.getDeclaredMethods());
						parameterizedSetMethod.invoke(parameterizedObject, arr[1]);
						
						values.add(parameterizedObject);
					}
					rr.close();
					reduceMethod.invoke(obj, parameter0, values.iterator(), collector);

					//add the resulting final key-value pairs to a hash table(mini-reduce)
					addCollectorValues(key, collector);
			
        		}
        		
    			//Write the reduced key-value pairs from the hash table to a single file.
        		RecordWriter rw = new RecordWriter(jobConf.getJobDir()+ "/" +jobConf.getOutputDirectoryPath()+"/finaloutput");
        		Enumeration keys = table.keys();
        		
        		while(keys.hasMoreElements()) {
        			Object key = keys.nextElement();
        			Object value = table.get(key);
        			
        			rw.writeRecord(key.toString() + "\t" + value.toString());
        		}
        		rw.close();
    		}
    		
    		//notify the data node that the thread has completed
    		listener.notifyOfReducerThreadCompletion(this);
  
    		//The exceptions notify the data node that the reducetask failed.
		} catch (IOException e) {
			listener.reduceTaskFailureDetected(task, e);
		} catch (ClassNotFoundException e) {
			listener.reduceTaskFailureDetected(task, e);
		} catch (InstantiationException e) {
			listener.reduceTaskFailureDetected(task, e);
		} catch (IllegalAccessException e) {
			listener.reduceTaskFailureDetected(task, e);
		} catch (IllegalArgumentException e) {
			listener.reduceTaskFailureDetected(task, e);
		} catch (InvocationTargetException e) {
			listener.reduceTaskFailureDetected(task, e);
		}
	
	}

	/**
	 * This method stores final key-value pairs in a hash table prior to writing them out to file.
	 * This is useful, since every there may be many intermediate files for the same key, so putting 
	 * them in a hashtable simplifies the process of accumulating values for identical keys, that may
	 * be coming from different chunks.
	 * 
	 * @param key - final key
	 * @param obj - An OutputColelctor object containing all the final key-value pairs for this key
	 */
	private void addCollectorValues(String key, Object obj) {
		OutputCollector collector = (OutputCollector) obj;
		List list = collector.getList();
		KeyValue keyVal = (KeyValue) list.get(0);
		
		//System.out.println("Key to be added "+key+ " Key value is " +keyVal.getKEY().toString() + keyVal.getVALUE().toString());
		
		if(table.get(key)==null) {
			table.put(key, keyVal.getVALUE());
			//System.out.println("Added new key value pair to table: "+ key.toString()+" "+keyVal.getVALUE().toString());
		}
		else {
			AddInterface value = (AddInterface) table.get(key);
			value.add(keyVal.getVALUE());
			table.put(key, value);
			//System.out.println("Updated existing key value pair in table to: "+ key.toString()+" "+keyVal.getVALUE().toString());
		}
		
	}

	/**
	 * 
	 * @param methods - The methods for the main class in the client' jar file.
	 * @return - The map method as defined as by the main class in the client's jar file.
	 */
	private Method getReduceMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("reduce")) {
				return methods[i];
			}
		}
		return null;	
	}

	public void addListener(DataNode dataNode) {
		listener = dataNode;
	}

	public ReduceTask getTask() {
		return task;
	}

	public void setTask(ReduceTask task) {
		this.task = task;
	}
	
	/**
	 * 
	 * @param methods - Array of methods for any MapReduce IO class.
	 * @return - Set method for the MapReduce IO class.
	 */
	private Method getSetMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("set")) {
				return methods[i];
			}
		}
		return null;
	}

}
