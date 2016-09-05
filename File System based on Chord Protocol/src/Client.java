

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

/**
* This class contacts the DHT, writes files into it and reads files from it.
*/

public class Client{
	public static void main(String args[]){
		try{
		
			//Setup call to SuperNode
			TTransport superNodeTransport = new TSocket(args[0], Integer.parseInt(args[1]));
			TProtocol superNodeProtocol = new TBinaryProtocol(new TFramedTransport(superNodeTransport));
			SuperNode.Client superNodeClient = new SuperNode.Client(superNodeProtocol);
			
			//Try to connect
			superNodeTransport.open();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String filename = null;	
			
			//Obtain the entry point into the DHT from the SuperNode
			String entryPoint = superNodeClient.getNode();
			superNodeTransport.close();
			String[] splits = entryPoint.split(":");
			
			//Menu-based program giving options for client to read files/write files and print logs
			while (true){
				int input = 0;
				System.out.println("Enter 1 for read and 2 for write.");
				System.out.println("Enter 3 for printing all logs");
				System.out.println("Enter 0 to quit");
				try{
					input = Integer.parseInt(br.readLine());
				}
				catch (Exception e){
					System.out.println("Option not an integer");
				}
				
				//If the client was to exit, then all logs and files written must be deleted
				if (input==0){
					superNodeTransport.open();
					superNodeClient.exitSystem();					
					superNodeTransport.close();					
					break;
				}				
				
				//Read a file from the DHT
				if (input == 1){
					System.out.println("Enter file name:");
					filename = br.readLine();
										
					TTransport nodeTransport = new TSocket(splits[0], Integer.parseInt(splits[1]));
					TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
					Node.Client nodeClient = new Node.Client(nodeProtocol);	
					nodeTransport.open();
					
					String contents = nodeClient.read(filename);
					
					if (contents.equals("")){
						System.out.println("File not found!");
					}
					else{
						System.out.println("File contents are: " + contents);
					}
					nodeTransport.close();
				}
				
				//Write a file into the DHT
				else if (input == 2){
					System.out.println("Enter file name:");
					filename = br.readLine();
					
					String contents = null;
					
					if (filename.contains(".")){
						contents = filename.substring(0, filename.indexOf('.'));
					}
					else{
						contents = filename;
					}
										
					TTransport nodeTransport = new TSocket(splits[0], Integer.parseInt(splits[1]));
					TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
					Node.Client nodeClient = new Node.Client(nodeProtocol);	
					nodeTransport.open();
					nodeClient.write(filename, contents);
					nodeTransport.close();
				}
				
				//Print all the logs
				else if (input == 3){					
					superNodeTransport.open();
					String logs = superNodeClient.getLogs();
					System.out.println(logs);
					superNodeTransport.close();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}