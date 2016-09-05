import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Threaded version of the client which runs multiple requests in different threads *
 */

public class ThreadedClient implements Runnable {
	private Thread t;
	
	//IP and Port of server it wishes to connect to
	private String IP;
	private int port;
	
	private int task; 				//Read or write task
	private String filename;		//Filename involved
	private int numberOfRequests;	//Number of such requests the client wishes to issue
	
	//Initialize parameters for the thread
	public ThreadedClient(String IP, int port, int task, String filename, int numberOfRequests){
		this.IP = IP;
		this.port = port;
		this.task = task;
		this.filename = filename;
		this.numberOfRequests = numberOfRequests;
	}
	
	@Override
	/**
	 * run() method of thread 
	 */
	public void run() {
		try{
			TTransport serverTransport = new TSocket(IP, port);
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
						
			for (int i=0;i<numberOfRequests;i++){
				serverTransport.open();				
				if (task == 1){					
					String contents = null;
					if (filename.contains(".")){
						contents = filename.substring(0, filename.indexOf('.'));
					}
					else{
						contents = filename;
					}
					String status = serverClient.write(filename, contents);
					System.out.println("Operation number " + (i+1) + " is a write. " + status);
				}
				else if (task == 0){
					String contents = serverClient.read(filename);
					System.out.println("Operation number " + (i+1) + " is a read and the contents are: " + contents);
				}
				
				serverTransport.close();
			}			
		}
		catch (Exception e){}		
	}
	
	public void start(){
		if (t == null){
			t = new Thread(this, "Thread"+IP+port);
			t.start();
		}
	}
}
