import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Class that implements the File Server service
 * Functionalities:
 * 1. Read a file from the system
 * 2. Write a file into the system
 * 3. Maintaining versions of all files on the server
 */

public class ServerHandler implements Server.Iface {
	
	//Self IP, Port and Coordinator's IP and Port
	String IP = null;
	String port = null;	
	String coordinator = null;
	
	//Map that maintains versions of all files on the server
	Map<String, Integer> versions = new ConcurrentHashMap<>();
	
	@Override
	public boolean ping() throws TException {
		return true;
	}
	
	@Override
	public Map<String, Integer> getVersionMap(){
		return versions;
	}

	@Override
	/**
	 * Method that initiates a write request by the client, for a file into the system
	 * Server contacts coordinator for the write quorum
	 * It then obtains version of the file (if it exists) from the servers in the quorum
	 * It updates the file and version number by writing into the quorum
	 */
	public String write(String filename, String contents) throws TException {
		
		String[] coordinatorSplits = coordinator.split(":");
		String coordinatorIP = coordinatorSplits[0];
		int coordinatorPort = Integer.parseInt(coordinatorSplits[1]);
		
		TTransport coordinatorTransport = new TSocket(coordinatorIP, coordinatorPort);
		TProtocol coordinatorProtocol = new TBinaryProtocol(new TFramedTransport(coordinatorTransport));
		Coordinator.Client coordinatorClient = new Coordinator.Client(coordinatorProtocol);
		coordinatorTransport.open();
		String quorum = coordinatorClient.assembleQuorum(IP, port, 1);
		coordinatorTransport.close();
		
		//System not ready to handle a request!
		if (quorum.equals("Not Ready")){
			return "System not ready!";
		}	
		
		int i=0;
		
		//Obtain versions of the file in all of the quorum
		String[] temp = quorum.split("~");
		int id = Integer.parseInt(temp[1]);
		String[] splits = temp[0].split(",");
		int[] allVersions = new int[splits.length];
		for (String server : splits){
			String[] tempSplit = server.split(":");
			if (tempSplit[0].equals(IP) && tempSplit[1].equals(port)){
				allVersions[i++] = getVersion(filename);
				continue;
			}
			TTransport serverTransport = new TSocket(tempSplit[0], Integer.parseInt(tempSplit[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			allVersions[i++] = serverClient.getVersion(filename);
			serverTransport.close();			
		}		
		
		//Find the latest version number of the file
		int index = findMaxIndex(allVersions);
		int version = allVersions[index];
		
		//Update the quorum with the a newer version of the file
		for (String server : splits){
			String[] tempSplit = server.split(":");
			if (tempSplit[0].equals(IP) && tempSplit[1].equals(port)){
				writeAux(filename, contents + "\nVersion: " + (version+1));
				continue;
			}
			TTransport serverTransport = new TSocket(tempSplit[0], Integer.parseInt(tempSplit[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			//Write the file to disk
			serverClient.writeAux(filename, contents + "\nVersion: " + (version+1));
			//Update version number at the servers in the quorum
			serverClient.updateVersion(filename, version+1);
			serverTransport.close();			
		}
		
		coordinatorTransport.open();
		//Invoke done() on the coordinator
		coordinatorClient.done(IP, port, 1, id);		
		coordinatorTransport.close();
		return "File " + filename + " written successfully!";
	}

	public void updateVersion(String filename, int version) {
		versions.put(filename, version);		
	}

	@Override
	/**
	 * Method that initiates a read request by the client, for a file in the system
	 * Server contacts coordinator for the read quorum
	 * It then obtains version of the file (if it exists) from the servers in the quorum
	 * If the file does not exist in the system, file not found message is returned to the client
	 * Else, it obtains the contents from the server with the latest version of the file
	 */
	public String read(String filename) throws TException {
		String[] coordinatorSplits = coordinator.split(":");
		String coordinatorIP = coordinatorSplits[0];
		int coordinatorPort = Integer.parseInt(coordinatorSplits[1]);
		
		//Obtain the quorum from coordinator
		TTransport coordinatorTransport = new TSocket(coordinatorIP, coordinatorPort);
		TProtocol coordinatorProtocol = new TBinaryProtocol(new TFramedTransport(coordinatorTransport));
		Coordinator.Client coordinatorClient = new Coordinator.Client(coordinatorProtocol);
		coordinatorTransport.open();
		String quorum = coordinatorClient.assembleQuorum(IP, port, 0);
		coordinatorTransport.close();		
		
		//System is not ready to handle a request!
		if (quorum.equals("Not Ready")){
			return "System not ready!";
		}
		
		int i=0;
		
		//Obtain version number of the file from servers in the quorum
		String[] temp = quorum.split("~");
		int id = Integer.parseInt(temp[1]);
		String[] splits = temp[0].split(",");
		int[] allVersions = new int[splits.length];
		for (String server : splits){
			String[] tempSplit = server.split(":");
			if (tempSplit[0].equals(IP) && tempSplit[1].equals(port)){
				allVersions[i++] = getVersion(filename);
				continue;
			}
			TTransport serverTransport = new TSocket(tempSplit[0], Integer.parseInt(tempSplit[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			allVersions[i++] = serverClient.getVersion(filename);
			serverTransport.close();			
		}
		
		// Now find the max version out of all the versions and contact that server for the file
		int index = findMaxIndex(allVersions);
		
		//File does not exist!
		if (allVersions[index] == 0){
			coordinatorTransport.open();
			coordinatorClient.done(IP, port, 0, id);
			coordinatorTransport.close();
			return "File not found!";
		}
		
		//Server with the latest version of the file
		String serverToContact = splits[index];
		String[] serverInfo = serverToContact.split(":");
		
		String contents = "";
		if (serverInfo[0].equals(IP) && serverInfo[1].equals(port)){
			contents = readAux(filename);
		}
		else{
			TTransport serverTransport = new TSocket(serverInfo[0], Integer.parseInt(serverInfo[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			//Read contents of file from disk
			contents = serverClient.readAux(filename);
			serverTransport.close();
		}
				
		coordinatorTransport.open();
		coordinatorClient.done(IP, port, 0, id);
		coordinatorTransport.close();
		
		return contents;
	}

	//Helper method to find index of the maximum element in an integer array
	private int findMaxIndex(int[] array) {
		int maxIndex = -1;
		int max = Integer.MIN_VALUE;
		for (int i=0;i<array.length;i++){
			if (array[i] > max){
				max = array[i];
				maxIndex = i;
			}
		}
		return maxIndex;
	}
	
	public int getVersion(String filename){
		if (versions.containsKey(filename)){
			return versions.get(filename);
		}
		else{
			return 0;
		}
	}

	@Override
	/**
	 * Method to write the file onto the disk
	 * Invoked by write() of server and synch() of coordinator
	 */
	public void writeAux(String filename, String contents) throws TException {
		String foldername = "folder_" + IP + "_" + port;
		File folder = new File(foldername);
		
		//Create the folder if it does not exist
		if (!folder.exists()) {
			if (folder.mkdir()) {						
				PrintWriter writer = null;
				try {
					writer = new PrintWriter(foldername+"/"+filename, "UTF-8");
				} catch (Exception e) {}
				writer.println(contents);						
				writer.close();
			} else {
				System.out.println("Failed to create directory!");
			}
		}
		else{
			PrintWriter writer = null;
			try {
				writer = new PrintWriter(foldername+"/"+filename, "UTF-8");
			} catch (Exception e) {} 
			writer.println(contents);						
			writer.close();
		}		
	}

	@Override
	/**
	 * Method to read the contents of a file from disk
	 * Invoked by read() of server
	 */
	public String readAux(String filename) throws TException {
		String foldername = "folder_" + IP + "_" + port;
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(foldername + "/" + filename));
			StringBuilder sb = new StringBuilder();
		    String line = br.readLine();
		    sb.append(line);
		    String contents = sb.toString();
		    br.close();		    		    
		    return contents;
		}
		catch (FileNotFoundException e){
			System.out.println("File was not found");
			return "File not found!";
		}
		catch (Exception e){} 	
		return "";
	}
	
	public void printVersionMap(){
		for (String filename: versions.keySet()){
			System.out.println(filename + " - Version: " + versions.get(filename));
		}
	}

	//Method to set self IP and Port
	public void setIPPort(String IP, String port) {
		this.IP = IP;
		this.port = port;
	}
	
	//Method to set the coordinator of the system
	public void setCoordinator(String coordinator) {
		this.coordinator = coordinator;		
	}
	
	
}