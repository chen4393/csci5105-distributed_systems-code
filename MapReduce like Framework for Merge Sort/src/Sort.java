import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.Arrays;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
Threaded class that performs the actual sorting
new thread is created each time a compute node receives a request
*/

public class Sort extends Thread {
	private String serverIP;
	private int serverPort;
	private String inputFile;
	private long offset;
	private long size;
	private int taskNumber;
	
	public Sort(String serverIP, int serverPort, String inputFile, long offset, long size, int taskNumber) {
		super();
		this.serverIP = serverIP;
		this.serverPort = serverPort;
		this.inputFile = inputFile;
		this.offset = offset;
		this.size = size;
		this.taskNumber = taskNumber;
	}
	
	public void run(){
		try{
			
			//maintain two pointers: actualOffset and rightPointer
			//initially at offset and offset+size 
			//keep moving them leftwards until they are pointing to a space
			//Read the bytes in between and convert them into the numbers needed
			

			long startTime = System.currentTimeMillis();
			
			RandomAccessFile file = new RandomAccessFile("input/" + inputFile, "r");
			file.seek(offset);
			
			long length = file.length();
			
			//System.out.println("offset: " + offset + " and size: " + size);
			
			long rightPointer = offset + size;
			long actualOffset = offset;
			
			byte[] b = new byte[1];
			char c;
			if (offset > 0){
				
				file.read(b, 0, 1);
				c = (char) (b[0] & 0xFF);
				while (c != ' '){
					actualOffset--;
					//sizeToRead++;
					file.seek(actualOffset);
					file.read(b, 0, 1);
					c = (char) (b[0] & 0xFF);
				}
				actualOffset++;
			}
			
			file.seek(offset + size);
			file.read(b, 0, 1);
			c = (char) (b[0] & 0xFF);
			
			while (c !=' '){
				rightPointer--;
				
				if (rightPointer == actualOffset){
					while (c != ' ' && rightPointer < length){
						rightPointer++;
						file.seek(rightPointer);
						file.read(b, 0, 1);
						c = (char)(b[0] & 0xFF);
					}
					break;
				}
				
				file.seek(rightPointer);
				file.read(b, 0, 1);
				c = (char) (b[0] & 0xFF);				
			}
			
			if (rightPointer == length){
				file.seek(rightPointer);
				file.read(b, 0, 1);
				c = (char)(b[0] & 0xFF);
				while (!(c >= '0' && c <= '9')){
					rightPointer--;
					file.seek(rightPointer);
					file.read(b, 0, 1);
					c = (char)(b[0] & 0xFF);
				}
			}
			
			
			
			byte[] array = new byte[(int) (rightPointer - actualOffset + 1)];		    

		    file.seek(actualOffset);
		    file.read(array, 0, (int)(rightPointer - actualOffset + 1));
		    file.close();
		    
		    String string = new String(array, "UTF-8");
		    String[] numbers = string.split(" ");
		    	    
		    int[] intChunk = new int[numbers.length];
		    int index = 0;
		    for (String number: numbers){
		    	intChunk[index++] = Integer.parseInt(number);
		    }	
		    
			//Perform a sort operation on them
			
			Arrays.sort(intChunk);
			
			
			// Open intermediate file and write the result
			String intermediateFile = "intermediate/sort" + taskNumber + ".txt";
			
			Writer w = new FileWriter(intermediateFile);
			for (int i : intChunk){
				w.write(String.valueOf(i));
				w.write(" ");
			}
			w.close();				
			

			//Inform the server that this task is done. 
			TTransport serverTransport = new TSocket(serverIP, serverPort);
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			serverTransport.open();
			serverClient.done(intermediateFile, taskNumber, 0);
			serverTransport.close();
			
			//Statistic for this sorting task
			System.out.println("Time taken for sort task: " + taskNumber + " = " + (System.currentTimeMillis() - startTime) + " ms");
			
		}
		catch (Exception e){			
			e.printStackTrace();			
		}
	}	
}
