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
 * Threaded class that starts the compute node service
 */

public class ComputeNodeServer extends Thread{
	private static double failureProb;
	
	//method that decides the failure of a compute node using the probability
	public void run(){
		while (true){
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
			double random = Math.random();
			if (random < failureProb){
				System.exit(0);
			}			
		}
	}
	
	public static void main(String arg[]){
		try{
			//The IP and port of the coordinator is passed as command-line arguments
			String IP = InetAddress.getLocalHost().getHostAddress();
			int port = Integer.parseInt(arg[0]);
			
			String serverIP = arg[1];
			int serverPort = Integer.parseInt(arg[2]);
			
			failureProb = Double.parseDouble(arg[3]);
			
			TTransport serverTransport = new TSocket(serverIP, serverPort);
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			serverClient.join(IP, port);
			serverTransport.close();
			
			TServerTransport transport = new TServerSocket(port);
			TTransportFactory factory = new TFramedTransport.Factory();

			//Create service request handler
			ComputeNodeHandler handler = new ComputeNodeHandler();
			ComputeNode.Processor processor = new ComputeNode.Processor(handler);
			
			handler.setParams(serverIP, serverPort, failureProb);
			
			new ComputeNodeServer().start();
			
			//Set server arguments
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
			args.processor(processor);	 //Set handler
			args.transportFactory(factory);  
			TServer server = new TThreadPoolServer(args);
			server.serve();
		}
		catch (Exception e){}
	}
}