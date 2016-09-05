import java.util.List;

import org.apache.thrift.TException;

/*
Class that implements the functions in the .thrift file
*/

public class ComputeNodeHandler implements ComputeNode.Iface{
	String serverIP;
	int serverPort;
	double failureProb;
	
	int numberOfTasks = 0;
	Object lock = new Object();
	
	@Override
	public boolean ping() throws TException {
		// TODO Auto-generated method stub
		return true;
	}	

	/*
	Method that creates a thread of sort class
	*/
	@Override
	public void startSort(String inputFile, long offset, long size, int taskNumber) throws TException {
		Sort task = new Sort(serverIP, serverPort, inputFile, offset, size, taskNumber);
		synchronized(lock){
			numberOfTasks++;
		}
		task.start();
		return;
	}

	/*
	Method that creates a thread of merge class
	*/
	@Override
	public void startMerge(List<String> fileList, int taskNumber) throws TException {
		// TODO Auto-generated method stub
		Merge task = new Merge(serverIP, serverPort, fileList, taskNumber);
		synchronized(lock){
			numberOfTasks++;
		}
		task.start();
		return;
	}
	
	public void setParams(String serverIP, int serverPort, double failureProb){
		this.serverIP = serverIP;
		this.serverPort = serverPort;
		this.failureProb = failureProb;
	}

	/*
	Method that prints statistics at this compute node
	*/
	@Override
	public void printStatistics() throws TException {
		System.out.println("Total number of tasks executed at this compute node = " + numberOfTasks);
	}
}