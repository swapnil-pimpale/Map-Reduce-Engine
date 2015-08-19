import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.List;


public class HeartBeatThread implements Runnable {
	private List<String> dataNodes;
	private String masterNode;
	private int registryPort;
	private MasterNode master;
	
	public HeartBeatThread(String masterNode, List<String> dataNodes, int registryPort, MasterNode master) {
		this.masterNode = masterNode;
		this.dataNodes = dataNodes;
		this.registryPort = registryPort;
		this.master = master;
	}
	@Override
	public void run() {
		Registry registry;
		CommunicatorInterface dataNodeCommunicator;
		
		while (true) {

			Iterator<String> itr = dataNodes.iterator();
			String node = null;
			
			while(itr.hasNext()) {

				try {
					node = itr.next();
					registry = LocateRegistry.getRegistry(node, registryPort);
					dataNodeCommunicator = (CommunicatorInterface) registry.lookup("communicator_" + node);
					String status = dataNodeCommunicator.getStatus();
					//System.out.println(node + "returned status " + status);
				} catch (RemoteException e) {
					String temp = node;
					itr.remove();
					System.out.println("Node "+temp+" failed.");
					master.failureDetected(temp);
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
						
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}

