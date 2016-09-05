

import org.apache.thrift.*;
import org.apache.thrift.server.*;

import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.*;

public class NodeServer{
	public static void main(String arg[]){
		try{
			//Create Thrift server socket
			TServerTransport serverTransport = new TServerSocket(Integer.parseInt(arg[0]));
			TTransportFactory factory = new TFramedTransport.Factory();

			//Create service request handler
			NodeHandler handler = new NodeHandler();
			Node.Processor processor = new Node.Processor(handler);

			//Set server arguments
			TServer.Args args = new TServer.Args(serverTransport);
			args.processor(processor);	 //Set handler
			args.transportFactory(factory);  //Set FramedTransport (for performance)

			//Run server as a single thread
			TServer server = new TSimpleServer(args);
			server.serve();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}
