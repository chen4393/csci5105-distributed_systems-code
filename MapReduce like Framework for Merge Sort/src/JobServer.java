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
 * Class that starts the job server service
 * parameters using chunkSize and mergeFileListSize
 */

public class JobServer{
			
	public static void main(String arg[]){
		try{
			//The IP and port of the coordinator is passed as command-line arguments
			String IP = InetAddress.getLocalHost().getHostAddress();
			int port = Integer.parseInt(arg[0]);
			
			int chunkSize = Integer.parseInt(arg[1]);
			int mergeFileListSize = Integer.parseInt(arg[2]);
			
			TServerTransport serverTransport = new TServerSocket(port);
			TTransportFactory factory = new TFramedTransport.Factory();

			//Create service request handler
			ServerHandler handler = new ServerHandler();
			Server.Processor processor = new Server.Processor(handler);
			
			handler.setParams(chunkSize, mergeFileListSize);
						
			//Set server arguments
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
			args.processor(processor);	 //Set handler
			args.transportFactory(factory);  

			TServer server = new TThreadPoolServer(args);
			server.serve();
		}
		catch (Exception e){}
	}
}
