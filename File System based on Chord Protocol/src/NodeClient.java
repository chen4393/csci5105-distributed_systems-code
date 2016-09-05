

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class NodeClient{
	public static void main(String args[]){
		try{
		
			String myIP = InetAddress.getLocalHost().getHostAddress();
			
			//Setup connection to SuperNode
			TTransport superNodeTransport = new TSocket(args[0], Integer.parseInt(args[1]));			
			TProtocol superNodeProtocol = new TBinaryProtocol(new TFramedTransport(superNodeTransport));			
			SuperNode.Client superNodeClient = new SuperNode.Client(superNodeProtocol);			
						
			superNodeTransport.open();			
			
			//Attempt to join the network
			//Obtain the list of nodes already existing in the system, along with the current node appended at the end
			String nodesList = superNodeClient.join(myIP, args[2]);
			
			//If the SuperNode is busy serving a join request by another node, then the current process goes to sleep for a second
			//and then attempts to join the system again
			while (nodesList.equals("NACK")){
				System.out.println("Super Node Busy. Trying to connect again..");
				Thread.sleep(1000);
				nodesList = superNodeClient.join(myIP, args[2]);
			}						
			
			//For each of the nodes in the system, call updateDHT() so that the finger tables and predecessors are modified
			//with the addition of the new node
			String[] nodes = nodesList.split(",");
			for (int i=0;i<nodes.length;i++){
				String[] nodeInfo = nodes[i].split(":");
				String IP = nodeInfo[0];
				String port = nodeInfo[1];
				TTransport nodeTransport = new TSocket(IP, Integer.parseInt(port));
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				Node.Client nodeClient = new Node.Client(nodeProtocol);
				nodeTransport.open();
				nodeClient.updateDHT(nodesList);
				nodeTransport.close();
			}			
			
			//Inform the SuperNode that the current node has joined the network and it can serve other reqests
			superNodeClient.postJoin(myIP, args[2]);
			superNodeTransport.close();
			
			//NodeClient provides an interface to print out all the details of the nodes
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			while (true){
				int input = 0;
				System.out.println("Enter 1 for printing structure and 0 to exit.");
				
				try{
					input = Integer.parseInt(br.readLine());
				}
				catch (Exception e){
					System.out.println("Option not an integer");
				}
				
				//Exit the Client
				if (input==0){
					break;
				}	

				//Print out the structure using getNodeDetails()
				else if (input == 1){
					TTransport nodeTransport = new TSocket(myIP, Integer.parseInt(args[2]));
					TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
					Node.Client nodeClient = new Node.Client(nodeProtocol);
					nodeTransport.open();
					String details = nodeClient.getNodeDetails();
					System.out.println(details);					
					nodeTransport.close();
				}											
			}			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}