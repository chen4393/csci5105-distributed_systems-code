import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
* Threaded class that performs the actual merging operation
*/

public class Merge extends Thread {
	private String serverIP;
	private int serverPort;
	private List<String> fileList;
	private int taskNumber;
		
	public Merge(String serverIP, int serverPort, List<String> fileList, int taskNumber) {
		super();
		this.serverIP = serverIP;
		this.serverPort = serverPort;
		this.fileList = fileList;
		this.taskNumber = taskNumber;
	}
	
	public void run(){
		try{
			
			long startTime = System.currentTimeMillis();
			
			List<Integer> merge1 = new ArrayList<>();
			List<Integer> merge2 = new ArrayList<>();
			

			//merge the files two at a time using standard merging procedure
			
			File file1 = new File(fileList.get(0));
			if (file1.exists()){
				BufferedReader br = new BufferedReader(new FileReader(file1));
				String line = br.readLine();
				br.close();
				
				if (line!=null){
					String[] numbers = line.split(" ");
									
					for (String number: numbers){
						merge1.add(Integer.parseInt(number));
					}
				}
			}
			
						
			for (int i=1;i<fileList.size();i++){
				File file2 = new File(fileList.get(i));
				if (file2.exists()){
					BufferedReader br = new BufferedReader(new FileReader(file2));
					String line = br.readLine();
					br.close();
					
					if (line!=null){
						String[] numbers = line.split(" ");											
						
						for (String number: numbers){
							merge2.add(Integer.parseInt(number));
						}				
					}
					
					List<Integer> mergeList = new ArrayList<>();
					int size1 = merge1.size();
					int size2 = merge2.size();
					
					int index1 = 0, index2 = 0;
					
					while (index1 < size1 && index2 < size2){
						if (merge1.get(index1) <= merge2.get(index2)){
							mergeList.add(merge1.get(index1));
							index1++;
						}
						else{
							mergeList.add(merge2.get(index2));
							index2++;
						}
					}
					
					while (index1 < size1){
						mergeList.add(merge1.get(index1++));
					}
					
					while (index2 < size2){
						mergeList.add(merge2.get(index2++));
					}
					
					merge1 = new ArrayList<>(mergeList);
					merge2.clear();
				}
				
			}
			
						
			String intermediateFile = "intermediate/merge" + taskNumber + ".txt";
			
			
			//Write the merged numbers into the file
			Writer w = new FileWriter(intermediateFile);
			for (int i : merge1){
				w.write(String.valueOf(i));
				w.write(" ");
			}
			w.close();
			
			//Inform the server that the merge task is done
			TTransport serverTransport = new TSocket(serverIP, serverPort);
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			serverClient.done(intermediateFile, taskNumber, 1);
			serverTransport.close();
			
			//print statistics for this merge task
			System.out.println("Time taken for merge task: " + taskNumber + " = " + (System.currentTimeMillis() - startTime) + " ms");
			
		}
		catch (Exception e){}
	}
	
}
