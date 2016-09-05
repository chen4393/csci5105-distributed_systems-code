import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


/**
* Class that implements the methods as part of the .thrift file
*/

public class ServerHandler extends Thread implements Server.Iface{
	
	//parameters
	private long chunkSize = 0;
	private int mergeFileListSize = 0;
	private List<ComputeNodeInfo> nodes = new ArrayList<>();
			
	private Map<SortTask, ComputeNodeInfo> sortAssignedTasks = new ConcurrentHashMap<>();
	private Map<MergeTask, ComputeNodeInfo> mergeAssignedTasks = new ConcurrentHashMap<>();
	
	@Override
	public boolean ping() throws TException {
		return true;
	}

	/*
	Method invoked when the client issues a job request
	*/
	@Override
	public String submitJob(String inputFile) throws TException {		
		
		int initialNodeCount = nodes.size();
		try{
	

			//Make temporary directory to store intermediate files
			new File("intermediate/").mkdir();
			
			long startTime = System.currentTimeMillis();
						
			RandomAccessFile file = new RandomAccessFile("input/" + inputFile, "r");
			long fileSize = file.length();
			
			int numberOfSortTasks = 0;
			int offset = 0;
			int taskNumber = 0;
			

			//Assign tasks to the compute nodes in a round robin fashion based on the offset and chunksize values
			//Try reaching the node, else deal with it in reassigning (fault tolerance)
			while (offset + chunkSize <= fileSize){
				
				ComputeNodeInfo node = nodes.get(taskNumber%nodes.size());
				SortTask task = new SortTask(inputFile, offset, chunkSize, taskNumber++);
				sortAssignedTasks.put(task, node);
				numberOfSortTasks++;
				System.out.println("Sort task: " + (taskNumber-1) + " sent to node: " + node);
				try{
					TTransport nodeTransport = new TSocket(node.getIP(), node.getPort());
					TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
					ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
					nodeTransport.open();				
					nodeClient.startSort(task.getFilename(), task.getOffset(), task.getSize(), task.getTaskNumber());
					nodeTransport.close();
					offset+=chunkSize;
				}
				catch (Exception e){
					offset+=chunkSize;
					continue;
				}				
			}
			
			//The last chunk of the file sent separately
			ComputeNodeInfo lastNode = nodes.get(taskNumber%nodes.size());
			SortTask lastTask = new SortTask(inputFile, offset, fileSize - offset, taskNumber++);
			sortAssignedTasks.put(lastTask, lastNode);
			numberOfSortTasks++;
			try{
				TTransport nodeTransport = new TSocket(lastNode.getIP(), lastNode.getPort());
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
				nodeTransport.open();				
				nodeClient.startSort(lastTask.getFilename(), lastTask.getOffset(), lastTask.getSize(), lastTask.getTaskNumber());
				nodeTransport.close();
													
			}
			catch (Exception e){}	
			
			
			file.close();
				
			
			// Reassigning the tasks if the node that was originally assigned to it is dead
			// Do this periodically (after every 5 seconds)
			while (!sortAssignedTasks.isEmpty()){
				//System.out.println("I am stuck in reassigning loop!");
				Thread.sleep(5000);
				for (SortTask task : sortAssignedTasks.keySet()){
					ComputeNodeInfo node = sortAssignedTasks.get(task);
					if (node == null){
						continue;
					}
					try{
						TTransport nodeTransport = new TSocket(node.getIP(), node.getPort());
						TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
						ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
						nodeTransport.open();
						nodeTransport.close();
					}
					catch (Exception e){
						//If node is dead, reassign it
						System.out.print("Node " + node + " is dead. Reassigning sort task: " + task + " to node: ");
						nodes.remove(node);
						for (ComputeNodeInfo reassignNode : nodes){
							try{
								TTransport nodeTransport = new TSocket(reassignNode.getIP(), reassignNode.getPort());
								TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
								ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
								nodeTransport.open();
								nodeClient.startSort(task.getFilename(), task.getOffset(), task.getSize(), task.getTaskNumber());
								nodeTransport.close();
								sortAssignedTasks.put(task, reassignNode);
								System.out.println(reassignNode);
								break;
							}
							catch (Exception e1){
								continue;
							}
						}
					}
				}
			}
			
			// Start merge tasks

			//System.out.println("Time for sorts: " + (System.currentTimeMillis() - startSort));
			//startSort = System.currentTimeMillis();
			//System.out.println("Number of sort tasks: " + numberOfSortTasks);
			
			System.out.println("--------------------------------------------------------------------------");
			System.out.println("Starting merge");
			System.out.println("--------------------------------------------------------------------------");
			
			//Calculate the number of passes for merge needed
			int noOfPasses = 0, temp = (int) numberOfSortTasks;
			while (temp >= 1){
				temp = temp / mergeFileListSize;
				noOfPasses++;
			}
			
			
			//Iterate over number of passes needed for merging all the files
			int numberOfMergeFiles = (int) numberOfSortTasks;
			int fileIndexOffset = 0;
			for (int pass = 0; pass<noOfPasses; pass++){
				int count = 0;
				int numberOfMerges = 0;
				
				//Number of files that is the outcome of this merge step
				if (numberOfMergeFiles % mergeFileListSize == 0){
					numberOfMerges = numberOfMergeFiles / mergeFileListSize;
				}
				else{
					numberOfMerges = (numberOfMergeFiles / mergeFileListSize)  + 1;
				}
				
				//Add the required files to each of the tasks
				for (int k = 0; k < numberOfMerges; k++){
					List<String> fileList = new ArrayList<>();
					for (int p = 0; p < mergeFileListSize; p++){
						count++;						
						if (pass == 0){
							fileList.add("intermediate/sort" + (fileIndexOffset++) + ".txt");
						}
						else{
							fileList.add("intermediate/merge" + (fileIndexOffset++) + ".txt");
						}
						if (count == numberOfMergeFiles){
							break;
						}
					}
					//Create a task object and send it to the corresponding node
					//Try, else deal with it while reassining
					ComputeNodeInfo node = nodes.get(k%nodes.size());
					MergeTask task = new MergeTask(fileList, taskNumber++);				
					mergeAssignedTasks.put(task, node);		
					System.out.println("Merge task " + (taskNumber - 1) + " sent to node: " + node );
					
					try{
						TTransport nodeTransport = new TSocket(node.getIP(), node.getPort());
						TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
						ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
						nodeTransport.open();
						nodeClient.startMerge(task.getFileList(), task.getTaskNumber());
						nodeTransport.close();
					}
					catch (Exception e){
						continue;
					}					
				}
				numberOfMergeFiles = numberOfMerges;
				
				
				System.out.println("Number of merges " + numberOfMerges + " merge files " + numberOfMergeFiles);
				
				//Loop which checks if all the merge tasks of this pass are done or not. 
				//Every 5 seconds --- hearbeat mechanism
				while (!mergeAssignedTasks.isEmpty()){
					Thread.sleep(5000);
					for (MergeTask task : mergeAssignedTasks.keySet()){
						ComputeNodeInfo node = mergeAssignedTasks.get(task);
						if (node == null){
							continue;
						}
						try{
							TTransport nodeTransport = new TSocket(node.getIP(), node.getPort());
							TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
							ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
							nodeTransport.open();
							nodeTransport.close();
						}
						catch (Exception e){

							//Reassign the task if the node is dead
							System.out.print("Node " + node + " is dead. Reassigning merge task: " + task + " to node: ");
							nodes.remove(node);
							for (ComputeNodeInfo reassignNode : nodes){
								try{
									TTransport nodeTransport = new TSocket(reassignNode.getIP(), reassignNode.getPort());
									TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
									ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
									nodeTransport.open();
									nodeClient.startMerge(task.getFileList(), task.getTaskNumber());
									nodeTransport.close();
									
									mergeAssignedTasks.put(task, reassignNode);
									System.out.println(reassignNode);
									break;
								}
								catch (Exception e1){
									continue;
								}
							}
						}
					}
				}
			}

			//Print statistics and final time taken
			System.out.println("\n\n\n\n");
			System.out.println("Job completed!!");
			System.out.println("Total time for job = " + (System.currentTimeMillis() - startTime) + " ms");
			System.out.println("\n\n\n\n");
			

			//Create output directory and copy the output file while deleting the others
			new File("output/").mkdir();
			Path source = Paths.get("intermediate/merge"+--taskNumber+".txt");
			Path dest = Paths.get("output/sorted_"+inputFile);
			try {
			   Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
			} catch (Exception e) {
			System.out.println("Copy failed");	   
			}

			File dir = new File("intermediate/");
			for (File fileToDelete: dir.listFiles())
			fileToDelete.delete();
			
		}
		catch (Exception e){}
		
		
		//Print statistics at each compute node if they are alive
		for (ComputeNodeInfo node : nodes){
			try {
				TTransport nodeTransport = new TSocket(node.getIP(), node.getPort());
				TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
				ComputeNode.Client nodeClient = new ComputeNode.Client(nodeProtocol);
				nodeTransport.open();
				nodeClient.printStatistics();
				nodeTransport.close();
			} 
			catch (Exception e) {continue;}
		}

		//Print information regarding the faults in the system
		System.out.println("Initial number of compute nodes in the system: " + initialNodeCount);
		System.out.println("Number of nodes that failed during job execution: " + (initialNodeCount - nodes.size()));
		
		
		return "sorted_"+inputFile;
	}
	
	public void setParams(int chunkSize, int mergeFileListSize){
		this.chunkSize = chunkSize;
		this.mergeFileListSize = mergeFileListSize;
	}

	//Invoked when a compute node joins the system
	//Store the node information
	@Override
	public void join(String IP, int port) throws TException {
		ComputeNodeInfo node = new ComputeNodeInfo(IP, port);
		nodes.add(node);
	}

	//When a task is over, inform the server and update the maps of the running tasks
	@Override
	public void done(String intermediateFile, int taskNumber, int type) throws TException {
		if (type == 0){
			SortTask taskToRemove = new SortTask("",0,0,taskNumber);
			System.out.println("Sort task " + taskNumber + " completed by node: " + sortAssignedTasks.get(taskToRemove));
			sortAssignedTasks.remove(taskToRemove);
		}
		else {
			MergeTask taskToRemove = new MergeTask(new ArrayList<String>(), taskNumber);
			System.out.println("Merge task " + taskNumber + " completed by node: " + mergeAssignedTasks.get(taskToRemove));
			mergeAssignedTasks.remove(taskToRemove);			
		}		
	}
}