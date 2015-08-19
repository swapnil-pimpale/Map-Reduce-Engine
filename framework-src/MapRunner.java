import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarFile;

/**
 * The MapRunner class is a thread that runs the map task on a particular chunk.
 * It uses reflection to construct an object of the main class present in the client's jar file,
 * and then calls the map method of the object on each line in the chunk to populate the output collector
 * with intermediate key-value pairs. Once the chunk has been processed, the intermediate key-value pairs
 * will be written to disk locally, sorted, partitioned, and sent to each of the appropriate reducer nodes.
 * 
 * A client's map method should be of the form: map(<Any MapReduce IO Class> key, Text value, 
<Any MapReduce IO Class, Any MapReduce IO Class>). This is explained in greater detail in the report.
 * @author Romit
 *
 */
public class MapRunner extends TaskRunner {

	private MapTask task;
	private DataNode listener;
	
	public MapRunner(MapTask task) {
		this.task = task;
	}

	@Override
	public void run() {
		JobConfiguration jobConf = task.getJobConf();
		int chunkID = task.getChunkID();
		
		try {
			//Read the main class from the jar file, and get the class
			JarFile jarFile = new JarFile(jobConf.getJobDir()+"/client.jar");
			String className = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
			File file  = new File(jobConf.getJobDir()+"/client.jar");
			URL url = file.toURI().toURL(); 
			URL[] urls = new URL[]{url};
			ClassLoader cl = new URLClassLoader(urls);
			Class c = cl.loadClass(className);

			//Construct the object of the main class, get the map method for the object
            Object obj = c.newInstance();
            Method[] methods = c.getDeclaredMethods();
            Method mapMethod = getMapMethod(methods);
            String nextRecord;
            
            //Get the classes for the map method's arguments, and construct objects from
            //those classes
            Class param1 = mapMethod.getParameterTypes()[1]; //this is always Text
            Class param2 = mapMethod.getParameterTypes()[2]; //this is always OutputCollector
            Object text = param1.newInstance(); //Text object
            Object collector = param2.newInstance(); //OutputCollector object
            
            //Every MapReduceIO class has a set method, since they all have empty constructors.
            RecordReader rr = new RecordReader(jobConf.getJobDir()+"/chunk-"+chunkID);
            methods = param1.getDeclaredMethods();
            //The text sext method is used to pass in lines to the map function
            Method textSetMethod = getTextSetMethod(methods);
            
            /**
             * Read a line from the chunk, and pass it into the map method using a Text object to wrap
             * the line. The OutputCollector object collects the intermediate key-value pairs that result 
             * from the map function call.
             */
    		while ((nextRecord = rr.getNextRecord()) != null) {
				textSetMethod.invoke(text, nextRecord);
				mapMethod.invoke(obj, null, text, collector);
    		}
    		rr.close();
    		
    		//Sort the resulting intermediate key-value pairs in the collector
    		List<?> list = null;    		
			methods = param2.getDeclaredMethods();
			Method collectorSortMethod = getCollectorSortMethod(methods);
    		collectorSortMethod.invoke(collector);
    		
    		//Return a list of the sorted intermediate key-value pairs
    		Method collectorGetListMethod = getCollectorListMethod(methods);
			list = (List) collectorGetListMethod.invoke(collector);
			
    		Iterator<?> itr = list.iterator();
    		RecordWriter rw;
    		
    		/**
    		 * For each key value pair, write it to an output file. Each unique key is written to it's own
    		 * output file. If a file already exists for that key, the write will simply append to that file.
    		 */
    		while(itr.hasNext()) {	
    			KeyValue<?, ?> pair = (KeyValue<?, ?>) itr.next();
    			Object key = pair.getKEY();
    			Object value = pair.getVALUE();
    			rw = new RecordWriter(jobConf.getJobDir()+"-intermediate/"+key.toString()+"-"+chunkID);
    			//System.out.println("Writing record "+key.toString()+" "+value.toString()+ " to" +
    			//jobConf.getJobDir()+"-intermediate/"+key.toString());
    			rw.writeRecord(key.toString()+"\t"+value.toString());
    			rw.close();		
    		}
    		
    		//notify the data node that the thread has completed
    		listener.notifyOfMapperThreadCompletion(this);    		
    		
    		//The exceptions notify the data node that the maptask failed.
		} catch (IOException e) {			
			listener.mapTaskFailureDetected(task, e);
		} catch (ClassNotFoundException e) {
			listener.mapTaskFailureDetected(task, e);
		} catch (InstantiationException e) {
			listener.mapTaskFailureDetected(task, e);
		} catch (IllegalAccessException e) {
			listener.mapTaskFailureDetected(task, e);
		} catch (IllegalArgumentException e) {
			listener.mapTaskFailureDetected(task, e);
		} catch (InvocationTargetException e) {
			listener.mapTaskFailureDetected(task, e);
		}
		
		
		
	}

	/**
	 * 
	 * @param methods - The methods for the OutputCollector class
	 * @return - The method that returns the list of key-value pairs
	 * collected by the output collector.
	 */
	private Method getCollectorListMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("getList")) {
				return methods[i];
			}
		}
		return null;
	}

	/**
	 * 
	 * @param methods - The methods for the OutputCollector class
	 * @return - The method that sorts the key-value pairs in the 
	 * OutputCollector.
	 */
	private Method getCollectorSortMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("sortByKeys")) {
				return methods[i];
			}
		}
		return null;
	}

	/**
	 * 
	 * @param methods - The methods for the Text class
	 * @return - The method that sets the String value in the Text class
	 */
	private Method getTextSetMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("set")) {
				return methods[i];
			}
		}
		return null;
	}

	/**
	 * 
	 * @param methods - The methods for the main class in the client' jar file.
	 * @return - The map method as defined as by the main class in the client's jar file.
	 */
	private Method getMapMethod(Method[] methods) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals("map")) {
				return methods[i];
			}
		}
		return null;
	}

	public void addListener(DataNode dataNode) {
		listener = dataNode;
	}

	public MapTask getMapTask() {
		return task;
	}
}
