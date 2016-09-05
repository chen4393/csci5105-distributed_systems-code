

import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.security.*;

public class SuperNodeHandler implements SuperNode.Iface {	

		List<NodeInfo> listOfJoinedNodes = new ArrayList<NodeInfo>();  	//List of nodes in the system
		Set<Integer> assignedIDs = new HashSet<>();						//Set of ID's already assigned -- used to resolve collision
		String nodeList = "";	
		boolean busy = false;											//Status of the SuperNode
		
		@Override
		public boolean ping() throws TException {
			return true;
		}

		/**
		* Method invoked by a NodeClient when it wishes to join the system. If the SuperNode is busy with another request, then it returns 'NACK'
		* else, it assigns an ID to the new ID and then returns an updated list of nodes back to the node
		*/
		@Override
		public String join(String IP, String port) throws TException {						
			
			if (busy==true){				
				return "NACK";
			}
			
			// Set flag
			busy = true;
			
			int ID = HashService.hash(IP + port);
			while (assignedIDs.contains(ID)){
				ID = (ID+1) % Constants.MAX_NODES.getValue();
			}		
						
			// Add it to list of nodes in the system
			NodeInfo info = new NodeInfo(IP, port, ID);
			listOfJoinedNodes.add(info);
			assignedIDs.add(ID);
			
			// Build return string
			nodeList += info.toString() + ",";
						
			return nodeList;
		}

		/**
		* Method to inform the SuperNode that a node has successfully joined the network and that it can serve other requests
		*/
		@Override
		public void postJoin(String IP, String port) throws TException {			
			busy=false;
		}

		/**
		* Method to return a random node from the nodes list, which serves as the entry point to the client for all read/write requests
		*/
		@Override
		public String getNode() throws TException {
			// TODO Auto-generated method stub
			int index = (int)(Math.random() * (listOfJoinedNodes.size()));
			return listOfJoinedNodes.get(index).toString();
		}

		/**
		* Method to assemble logs from all nodes and return it (display) to the client upon request
		* The SuperNode contacts each node for its logs, via the getLogs() method
		*/
		@Override
		public String getLogs() throws TException {
			String logs = "";
			for (NodeInfo node: listOfJoinedNodes){
				TTransport nodeTransport = new TSocket(node.getIP(), Integer.parseInt(node.getPort()));
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				Node.Client nodeClient = new Node.Client(nodeProtocol);
				nodeTransport.open();
				logs += "Log for node " + node.getID() + ":\n\n";
				logs += nodeClient.getLogs();
				logs += "-------------------------------------------\n";
				nodeTransport.close();
			}
			return logs;
		}

		/**
		* Method called when the Client exits the system. The SuperNode contacts each node and subsequently they
		* remove all folders and files created by them during the lifetime of the program (cleanup() method).
		*/
		@Override
		public void exitSystem() throws TException {
			for (NodeInfo node: listOfJoinedNodes){
				TTransport nodeTransport = new TSocket(node.getIP(), Integer.parseInt(node.getPort()));
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				Node.Client nodeClient = new Node.Client(nodeProtocol);
				nodeTransport.open();
				nodeClient.cleanup();
				nodeTransport.close();
			}
		}
     
}
