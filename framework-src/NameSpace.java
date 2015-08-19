/**
 * Class NameSpace:
 * Keeps metadata about the location of chunks and their association with the jobs and nodes
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class NameSpace {
	//Map from JobId to chunk list	
	private HashMap<Integer, List<Chunk>> map1;
	//Map from ChunkID to node list
	private HashMap<Integer, List<String>> map2;
	
	public NameSpace() {
		map1 = new HashMap<Integer, List<Chunk>>();
		map2 = new HashMap<Integer, List<String>>();
	}
	
	public void addToMap1(int jobID, Chunk chunk) {
		List<Chunk> chunks = map1.get(jobID);
		if(chunks!=null) {
			chunks.add(chunk);
		}
		else {
			chunks = new ArrayList<Chunk>();
			chunks.add(chunk);
			map1.put(jobID, chunks);
		}
	}
	
	public void addToMap2(int chunkID, String node) {
		List<String> nodes = map2.get(chunkID);
		
		if (nodes != null) {
			nodes.add(node);
		} else {
			nodes = new ArrayList<String>();
			nodes.add(node);
			map2.put(chunkID, nodes);
		}
	}
	
	public void addNodeListToMap2(int chunkID, List<String> nodes) {
		if(map2.get(chunkID)==null) {
			map2.put(chunkID, nodes);
		}
		else {
			map2.get(chunkID).addAll(nodes);
		}
	}
	
	public void addChunkListToMap1(int jobID, List<Chunk> chunks) {
		if(map1.get(jobID)==null) {
			map1.put(jobID, chunks);
		}
		else {
			map1.get(jobID).addAll(chunks);
		}
	}
	
	public List<Chunk> getChunkListFromMap1(int jobID) {
		return map1.get(jobID);
	}
	
	public List<String> getNodeListFromMap2(int chunkID) {
		return map2.get(chunkID);
	}
	
	public void removeFromMap1(int jobID, Chunk chunk) {
		List<Chunk> chunks = map1.get(jobID);	
		if(chunks!=null) {
			chunks.remove(chunk);
		}	
	}
	
	public void removeJobFromMaps(int jobID) {
		List<Chunk> chunkList = map1.get(jobID);
		if(chunkList!=null) {
			for(Chunk chunk : chunkList) {
				map2.remove(chunk.getChunkID());
			}
		}
		map1.remove(jobID);
	}
}
