import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Class that starts a file server in the system
 * It is made multi-threaded to listen to multiple clients for requests at the same time
 * A separate thread exists to print out the file list at the server
 */

public class FileServer extends Thread{
	
	private static ServerHandler handler = null; 
	
	//run method of Thread that prints out file list
	public void run(){
		int option = 1;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true){
			System.out.print("Press 1 for printing file list and version numbers at server: ");
			try {
				option = Integer.parseInt(br.readLine());
			} catch (Exception e) {} 
			if (option == 1){
				handler.printVersionMap();
				System.out.println("----------------------- \n");
			}
		}	
	}
	
	public static void main(String arg[]){
		try{
			//The IP and port of the coordinator is passed as command-line arguments
			String IP = InetAddress.getLocalHost().getHostAddress();
			String port = arg[0];
			
			String coordinatorIP = arg[1];
			String coordinatorPort = arg[2];
			
			//Inform the coordinator that the server has joined
			TTransport coordinatorTransport = new TSocket(coordinatorIP, Integer.parseInt(coordinatorPort));
			TProtocol coordinatorProtocol = new TBinaryProtocol(new TFramedTransport(coordinatorTransport));
			Coordinator.Client coordinatorClient = new Coordinator.Client(coordinatorProtocol);
			coordinatorTransport.open();
			coordinatorClient.join(IP, port);
			coordinatorTransport.close();
			
			TServerTransport serverTransport = new TServerSocket(Integer.parseInt(port));
			TTransportFactory factory = new TFramedTransport.Factory();

			//Create service request handler
			handler = new ServerHandler();
			Server.Processor processor = new Server.Processor(handler);
			
			new FileServer().start();
					
			//Initialize self parameters
			handler.setIPPort(IP, port);			
			handler.setCoordinator(coordinatorIP + ":" + coordinatorPort);
						
			//Set server arguments
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
			args.processor(processor);	 //Set handler
			args.transportFactory(factory);  //Set FramedTransport (for performance)

			TServer server = new TThreadPoolServer(args);
			server.serve();
		}
		catch (Exception e){}
	}
}
