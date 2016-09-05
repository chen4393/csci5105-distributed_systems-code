import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Server class that initializes a coordinator server in the system
 * This is multi-threaded to allow for listening to requests from the clients as well as
 * requests from other servers for coordinator-specific purposes
 */
public class CoordinatorServer extends Thread{
	
	public static CoordinatorHandler handler = null;
	
	//run() function for the thread that performs the synch operation periodically every 10 seconds
	public void run(){
		try{
			while (true){				
				handler.synch();
				System.out.println("Synch operation done!");
				Thread.sleep(10000);
			}
		} catch (Exception e){}		
	}
	
	public static void main(String arg[]){
		try{
			//Create Thrift server socket
			TServerTransport serverTransport = new TServerSocket(Integer.parseInt(arg[0]));
			TTransportFactory factory = new TFramedTransport.Factory();

			//Read system parameters
			int n = Integer.parseInt(arg[1]);
			int nr = Integer.parseInt(arg[2]);
			int nw = Integer.parseInt(arg[3]);			
			
			//Check violation of Gifford's protocol!
			if (!(nr + nw > n) || !(nw > n/2)){
				System.out.println("Choice of n, nr, nw violates Gifford's protocol! Exiting");
				System.exit(0);
			}
			
			//Create service request handler
			handler = new CoordinatorHandler();
			Coordinator.Processor processor = new Coordinator.Processor(handler);
			
			List<Integer> params = new ArrayList<>();
			params.add(nr); params.add(nw); params.add(n);
			handler.setParameters(params);
			
			new CoordinatorServer().start();
			
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