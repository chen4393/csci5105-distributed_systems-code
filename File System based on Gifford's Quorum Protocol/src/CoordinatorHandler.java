import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * This class implements the Coordinator service of the system 
 * Functionalities: 
 * 1. Coordinate the joining process of all servers into the system
 * 2. Assemble read and write quorum for each request and return it to the server that received them
 * 3. Enforces concurrency control using a thread-safe queue data structure
 * 4. Performs synch operation in the background that keeps all files in the latest version at each server
 */

public class CoordinatorHandler implements Coordinator.Iface {
	
	//System parameters N, N_R, N_W
	int nr = 0;
	int nw = 0;
	int n = 0;
	
	int serversConnected = 0;  //number of servers connected
	String nodesList = "";	   //list of all servers in the system		
	int runningReads = 0; 	   //number of concurrent reads being processed
	
	ConcurrentLinkedQueue<Request> queue = new ConcurrentLinkedQueue<Request>(); //thread-safe queue
	private final Object lock = new Object();  //lock for atomic operations on shared variables
	int requestCount = 0;					   //number of requests the coordinator has received since beginning of time
	
	@Override
	public boolean ping() throws TException {
		return true;
	}

	@Override
	/**
	 * Method to assemble the quorum based on the request type and values of N_R, N_W
	 * IP and port are those of the server that contacts the coordinator with the request
	 */
	public String assembleQuorum(String IP, String port, int task) throws TException {
		
		//System is not yet ready!
		if (serversConnected != n){
			System.out.println("Number of servers connected is not equal to " + n + "!");
			return "Not Ready";
		}
		
		//Create a new Request object and enqueue it!
		Request request = null;
		synchronized(lock){
			requestCount++;
			request = new Request(IP, Integer.parseInt(port), task, requestCount);
		}		
		
		queue.add(request);		
		
		//Wait until you are the head of the queue, to be processed!
		while (queue.peek().getId() != request.getId()){
			Thread.yield();
		}
		
		//Enable concurrent reads and sequential writes using the counter runningReads
		if (task == 0){
			synchronized(lock){
				runningReads++;
			}
			queue.remove();
		}
		else if (task == 1){
			while (runningReads != 0){
				Thread.yield();
			}
		}
		
		//Assemble the quorum
		int quorumSize = -1;
		if (task == 0){
			quorumSize = nr;
		}
		else if (task == 1){
			quorumSize = nw;
		}
		
		StringBuilder returnVal = new StringBuilder();
		
		String[] nodeSplits = nodesList.split(",");
		int noOfNodes = nodeSplits.length;
		
		int count = 0;
		
		//Randomly select from nodesList for the servers to be part of the quorum
		ArrayList<Integer> selected = new ArrayList<Integer>();
		while (count < quorumSize){
			int random = (int) (Math.random() * (noOfNodes));
			if (!selected.contains(random)){
				selected.add(random);				
				returnVal.append(nodeSplits[random] + ",");
				count++;
			}
		}
		
		//Return the quorum back to the requesting server along with the request ID 
		return returnVal.toString() + "~" + request.getId();
	}

	@Override
	/**
	 * Synch method that runs in the background periodically, 
	 * and ensures that all files are up to date with the latest version in all servers 
	 */
	public void synch() throws TException {
		
		//System not yet ready to synch!
		if (serversConnected != n){
			System.out.println("Number of servers connected is not equal to " + n + "!");
			return ;
		}
		
		String[] nodeSplits = nodesList.split(",");
		List<Map<String, Integer>> allMaps = new ArrayList<>();
		
		//Obtain the versions map from all of the servers
		for (String server: nodeSplits){
			String[] splits = server.split(":");
			TTransport serverTransport = new TSocket(splits[0], Integer.parseInt(splits[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			allMaps.add(serverClient.getVersionMap());
			serverTransport.close();	
		}
		
		//All the unique files in the entire system
		List<String> uniqueFiles = getUniqueFilenames(allMaps);
		
		//Update each file by first obtaining maximum version from each of the servers that have them
		//Then for those that have an older version, write the file with the updated version number
		for (String filename: uniqueFiles){
			String contents = null;
			if (filename.contains(".")){
				contents = filename.substring(0, filename.indexOf('.'));
			}
			else{
				contents = filename;
			}
			
			List<Integer> fileVersions = new ArrayList<>();
			
			for (Map<String, Integer> map: allMaps){
				if (map.containsKey(filename)){
					fileVersions.add(map.get(filename));
				}
				else{
					fileVersions.add(0);
				}
			}
			
			int index = findMaxIndex(fileVersions);
			int maxVersion = fileVersions.get(index);
				
			//Write the file on the servers with an older version of the file only
			for (int i=0;i<fileVersions.size();i++){
				int version = fileVersions.get(i);
				if (version < maxVersion){
					String server = nodeSplits[i];
					String[] splits = server.split(":");
					TTransport serverTransport = new TSocket(splits[0], Integer.parseInt(splits[1]));
					TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
					Server.Client serverClient = new Server.Client(serverProtocol);
					serverTransport.open();
					int serverVersion = serverClient.getVersion(filename);
					if (serverVersion < maxVersion){
						//Logging the synch and update activity!
						System.out.println("Syncing file " + filename + " at server: " + splits[0] + ":" + splits[1]);
						serverClient.writeAux(filename, contents + "\nVersion: " + (maxVersion));
						serverClient.updateVersion(filename, maxVersion);
					}
					serverTransport.close();
				}
			}
		}
		
	}

	//Helper method to find the index of the largest element of an integer list
	private int findMaxIndex(List<Integer> list) {
		int maxIndex = -1;
		int max = Integer.MIN_VALUE;
		for (int i=0;i<list.size();i++){
			if (list.get(i) > max){
				max = list.get(i);
				maxIndex = i;
			}
		}
		return maxIndex;
	}

	//Helper method to find the unique files in the entire system based on the 
	//versions map of each of them
	private List<String> getUniqueFilenames(List<Map<String, Integer>> allMaps) {
		List<String> result = new ArrayList<>();
		for (Map<String, Integer> map: allMaps){
			for (String filename: map.keySet()){
				if (!result.contains(filename)){
					result.add(filename);		
				}
			}
		}
		return result;
	}

	@Override
	/**
	 * Method that is invoked by the server once it has finished processing a request
	 * If the request was a read, then decrement the runningReads counter
	 * Else, remove the head of the queue and return
	 */
	public void done(String IP, String port, int task, int id) throws TException {		
		if (task == 0){
			synchronized(lock){
				runningReads--;
			}
		}
		else if (task == 1){
			if (queue.peek().getId() == id){
				queue.remove();
			}
			else{
				System.out.println("Error!");
			}
		}		
	}
	
	/**
	 * Method invoked while initializing the coordinator with the system parameters 
	 */
	public void setParameters(List<Integer> params) {
		this.nr = params.get(0);
		this.nw = params.get(1);
		this.n = params.get(2);
	}

	@Override
	/**
	 * Method invoked by the server once it has joined the network, with its IP and port information
	 * nodesList is updated with this new information
	 */
	public void join(String IP, String port) throws TException {
		nodesList += IP+":"+port+",";
		serversConnected++;
	}
}