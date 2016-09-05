

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class NodeHandler implements Node.Iface {
		NodeInfo self = null;											//Current node as a NodeInfo object
		Map<Integer, NodeInfo> fingerTable = new HashMap<>();			//Finger table of current node
		NodeInfo predecessor = null;									//Predecessor of the current node in the system

		@Override
		public boolean ping() throws TException {
			return true;
		}

		/**
		* Method to update the finger table and predecessor fields once a new node has joined the network
		* If the current node is a new node itself, then first set 'self' object and then update
		* the finger table
		*/
		@Override
		public void updateDHT(String nodesList) throws TException {
			
			String[] nodes = nodesList.split(",");
			
			//Get the existing nodes in the network
			TreeSet<NodeInfo> IDs = getIDs(nodes);
			
			//Current node's info
			if (self == null){
				String selfNode = nodes[nodes.length-1];
				String[] selfInfo = selfNode.split(":");
				self = new NodeInfo(selfInfo[0], selfInfo[1], Integer.parseInt(selfInfo[2]));
			}
			
			//Find predecessor of the current node
			findPredecessor(IDs);
			
			//Build finger table for current node
			//Number of entries = m = log2(MAX_NODES)
			
			int entries = (int) Math.ceil(Math.log(Constants.MAX_NODES.getValue()) / Math.log(2));
			for (int i = 0; i<entries; i++){
				//Find the successor of the identifier (id + 2^(i)) in the system
				NodeInfo succ = findSuccessor(IDs, self.getID() + (int)Math.pow(2, i));
				fingerTable.put(i, succ);
			}			
		}

		/**
		* This method finds the predecessor of the current node using the list of the nodes in the system (in a sorted fashion)
		* If the current node has the least ID, then it's predecessor is the node with the highest ID. 
		* Else the predecessor is the node with the previous ID
		*/
		private void findPredecessor(TreeSet<NodeInfo> IDs) {
			try {
				// Search for self ID
				NodeInfo[] nodes = IDs.toArray(new NodeInfo[IDs.size()]);
				for (int i = 0; i < nodes.length; i++) {
					if (nodes[i].getID() == self.getID()) {
						if (i == 0) {
							predecessor = nodes[nodes.length - 1];
						} else {
							predecessor = nodes[i - 1];
						}
					}
				} 
			} catch (Exception e) {
				e.printStackTrace();
			}		
		}

		/**
		* Method to find the successor of the identifier 'key' in the system. 		
		*/
		private NodeInfo findSuccessor(TreeSet<NodeInfo> IDs, int key) {
			Iterator<NodeInfo> it = IDs.iterator();
			key = key % Constants.MAX_NODES.getValue();
			while (it.hasNext()){
				NodeInfo element = it.next();
				if (element.compareTo(new NodeInfo(null, null, key)) != -1){
					return element;
				}
			}
			it = IDs.iterator();
			return it.next();
		}

		/**
		* Method to get a sorted set of IDs in the system
		*/
		private TreeSet<NodeInfo> getIDs(String[] nodes) {
			TreeSet<NodeInfo> IDs = new TreeSet<>();
			for (int i = 0; i<nodes.length; i++){
				String[] splits = nodes[i].split(":");
				IDs.add(new NodeInfo(splits[0], splits[1], Integer.parseInt(splits[2])));
			}
			return IDs;
		}

		/**
		* Method to find the next hop in the routing of a read/write request, based on the key value
		* and the current node. This is done by analyzing the finger table for the next hop
		*/
		private NodeInfo findNextNode(int key) {
			int destination = -1;
			
			//We should complete a cycle
			if (key < self.getID()){
				key = key + Constants.MAX_NODES.getValue();
			}
			
			//Look through the finger table
			for (int i=0;i<fingerTable.size();i++){
				NodeInfo current = fingerTable.get(i);
				int currentID = current.getID();
				if (currentID < self.getID()){
					currentID += Constants.MAX_NODES.getValue();
				}
				if (currentID > self.getID() && currentID <= key){
					//update destination
					if (currentID > destination){
						destination = i;
					}
				}
			}
			if (destination == -1)
				return fingerTable.get(0);
			
			return fingerTable.get(destination);
		}
		
		/**
		* Method that determines whether the current node is responsible for key, and if not it returns the
		* next node in the routing path, by invoking findNextNode()
		*/
		public NodeInfo search(int key){	
			
			//Check predecessor
			int temp = self.getID();
			if (predecessor.getID() > temp){
				if (key <= temp)
					return null;
				temp = Constants.MAX_NODES.getValue() + temp;
			}
			//Check if the key lies in this range
			if (key > predecessor.getID() && key <= temp){
				return null;
			}
			else{
				NodeInfo next = findNextNode(key);
				return next;
			}					
		}
		
		/**
		* Auxiliary method to write() which maintains a third parameter visitedNodes, a list of all the nodes visited
		* during the routing of the request from the entry point into the system, till the destination.
		* This is a recursive method, which determines first if the current node is the one responsible for key,
		* else it forwards the request to the next hop. If the former, then it creates a folder for the current node,
		* writes the requested file with contents, and also creates an activity.log file, that logs information
		* pertaining to the completed request. It includes a timestamp, file details as well visitedNodes
		*/
		@Override
		public void writeAux(String filename, String contents, List<Integer> visitedNodes) throws TException {
			
			int key = HashService.hash(filename);
			System.out.println("Hashed key of filename " + filename + " = " + key);
			
			visitedNodes.add(self.getID());
			NodeInfo destination = search(key);
			
			if (destination == null){
				
				// Create directory (if needed) and then write file
				String foldername = "folder" + self.getID();
				String logFile = foldername+"/"+"activity.log";
				File folder = new File(foldername);
				
				//Create the folder if it does not exist
				if (!folder.exists()) {
					if (folder.mkdir()) {						
						PrintWriter writer = null;
						try {
							writer = new PrintWriter(foldername+"/"+filename, "UTF-8");
						} catch (Exception e) {							
							e.printStackTrace();
						} 
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
					} catch (Exception e) {							
						e.printStackTrace();
					} 
					writer.println(contents);						
					writer.close();
				}
				
				//Write (Update) the log file 
				PrintWriter writer = null;
				try {
					writer = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
				} catch (Exception e) {							
					e.printStackTrace();
				}
				
				//Timestamp
				DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				Date date = new Date();
				
				writer.println("----------------------------------------------------");
				writer.println(dateFormat.format(date) + ":");
				writer.println("Request for write completed successfully...");
				writer.println("Filename: " + filename + " written");
				writer.println("Nodes visited to write the file: ");
				
				for (int i = 0; i<visitedNodes.size() - 1; i++){
					writer.print(visitedNodes.get(i) + " -> ");
				}
				writer.println(self.getID());
				writer.println("\n");
				writer.close();
			}
			
			else{
			
				//forward the request to the next hop in the route
				TTransport nodeTransport = new TSocket(destination.getIP(), Integer.parseInt(destination.getPort()));
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				Node.Client nodeClient = new Node.Client(nodeProtocol);
				nodeTransport.open();
				
				//invoke writeAux() of the new node
				nodeClient.writeAux(filename, contents, visitedNodes);
				
				nodeTransport.close();
			}			
		}
		
		/**
		* Auxiliary method to read() which maintains a third parameter visitedNodes, a list of all the nodes visited
		* during the routing of the request from the entry point into the system, till the destination.
		* This is a recursive method, which determines first if the current node is the one responsible for key,
		* else it forwards the request to the next hop. If the former, then it attempts to find the file. 
		* If successful, then it returns the contents of the file, else it returns an error message to the client
		* "File not found!". It further updates activity.log of the corresponding node with details of this read request
		*/
		@Override
		public String readAux(String filename, List<Integer> visitedNodes) throws TException {
			int key = HashService.hash(filename);
			System.out.println("Hashed key of filename " + filename + " = " + key);
			
			visitedNodes.add(self.getID());
			NodeInfo destination = search(key);
			
			if (destination == null){
				// Read file and return contents			
				String foldername = "folder" + self.getID();
				String logFile = foldername+"/activity.log";
				BufferedReader br;
				try {
					br = new BufferedReader(new FileReader(foldername + "/" + filename));
					StringBuilder sb = new StringBuilder();
				    String line = br.readLine();

				    while (line != null) {
				        sb.append(line);
				        sb.append(System.lineSeparator());
				        line = br.readLine();
				    }
				    String contents = sb.toString();
				    br.close();
				    
				    PrintWriter writer = null;
					try {
						writer = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
					} catch (Exception e) {							
						e.printStackTrace();
					}
					
					//Update activity.log
					//Timestamp
					DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
					Date date = new Date();
					
					writer.println("----------------------------------------------------");
					writer.println(dateFormat.format(date) + ":");
					writer.println("Request for read completed successfully...");
					writer.println("Filename: " + filename + " read");
					writer.println("Nodes visited to read the file: ");
					
					for (int i = 0; i<visitedNodes.size() - 1; i++){
						writer.print(visitedNodes.get(i) + " -> ");
					}
					writer.println(self.getID());
					writer.println("\n");
					writer.close();
				    
				    return contents;
				}
				catch (FileNotFoundException e){
					System.out.println("File was not found");
					return "";
				}
				catch (Exception e){
					e.printStackTrace();
				} 	
				return "";
			}
			else{
				
				//Setup connection to the next hop in the route
				TTransport nodeTransport = new TSocket(destination.getIP(), Integer.parseInt(destination.getPort()));
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				Node.Client nodeClient = new Node.Client(nodeProtocol);
				nodeTransport.open();
				
				//Invoke readAux() on the next hop
				String contents = nodeClient.readAux(filename, visitedNodes);
				nodeTransport.close();
				return contents;
			}	
		}

		/**
		* Method that invokes writeAux() to achieve write functionality
		*/
		@Override
		public void write(String filename, String contents) throws TException {
			writeAux(filename, contents, new ArrayList<Integer>());			
		}

		/**
		* Method that invokes readAux() to achieve read functionality
		*/
		@Override
		public String read(String filename) throws TException {
			return readAux(filename, new ArrayList<Integer>());
		}

		/**
		* Method to return the details of the node which are:
		* Node ID, predecessor, successor, range of keys the node is responsible for, and its finger table
		* NodeClient invokes this method and this provides functionality of determining structure of DHT
		*/
		@Override
		public String getNodeDetails() throws TException {
			String result = "Node ID: " + self.getID() + "\n";
			if (predecessor.getID() > self.getID()){
				result += "Range of keys: 0-" + self.getID() + " & " + (predecessor.getID()+1) + "-" + Constants.MAX_NODES.getValue() + "\n";
			}
			else if (predecessor.getID() < self.getID()){
				result += "Range of keys: " + (predecessor.getID()+1) + "-" + self.getID() + "\n";
			}
			else{
				result += "Range of keys: 0-" + Constants.MAX_NODES.getValue() + "\n";
			}
			ArrayList<String> fileList = getFileList();
			result += "Predecessor: " + predecessor.getID() + "\n";
			result += "Successor: " + fingerTable.get(0).getID() + "\n";
			result += "Number of files stored: " + fileList.size() + "\n";
			result += "File list: \n";
			result += fileList + "\n";
			result += "Finger table: \n";
			result += "-----------------------------\n";
			
			Iterator<Integer> it = fingerTable.keySet().iterator();
			while (it.hasNext()){
				int key = it.next();
				result += key + "\t" + fingerTable.get(key) + "\n";
			}
			
			return result;
		}

		/**
		* Method to get the list of files the current node has written. They reside in the folder corresponding to the node
		*/
		private ArrayList<String> getFileList() {
			ArrayList<String> filenames = new ArrayList<String>();
			String foldername = "folder" + self.getID();
			try{
				File folder = new File(foldername);
				File[] listOfFiles = folder.listFiles();
				
				for (int i=0;i<listOfFiles.length;i++){
					if (listOfFiles[i].isFile() && !listOfFiles[i].getName().equals("activity.log")){
						filenames.add(listOfFiles[i].getName());
					}
				}
			}
			catch (Exception e){
				filenames.add("No files");
			}		
			
			return filenames;
		}

		/**
		* Method to return the contents of activity.log of the current node. This is collected by SuperNode
		* which assembles all logs and prints it out to the Client
		*/
		@Override
		public String getLogs() throws TException {
			String log = "";
			String foldername = "folder" + self.getID();
			
			try{
				BufferedReader br = new BufferedReader(new FileReader(foldername + "/activity.log"));
				String line;
				while ((line = br.readLine())!=null){
					log += line + "\n";
				}
				br.close();
			}
			catch (Exception e){}
			
			return log;
		}

		/**
		* When the client wishes to exit the system, the SuperNode contacts each node in the system and 
		* all files (and logs) written during the lifetime of the program are cleared
		*/
		@Override
		public void cleanup() throws TException {
			String foldername = "folder" + self.getID();			
			File folder = new File(foldername);
			
			if (folder.exists()) {
				deleteFolder(folder);
			}			
		}
		
		/**
		* Method to delete the folder written to the disk by the current node
		*/
		private static void deleteFolder(File folder) {
		    if (folder.exists()){
		        File[] listOfFiles = folder.listFiles();
		        if (listOfFiles != null){
		            for (int i=0; i<listOfFiles.length; i++) {
		                listOfFiles[i].delete();		             
		            }
		        }
		    }
		    folder.delete();
		}
}