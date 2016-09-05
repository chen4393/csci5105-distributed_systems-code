import java.util.Scanner;

import javax.swing.plaf.synth.SynthSeparatorUI;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
* This class issues job request to job server
* It is returned with the filename of sorted output
*/

public class Client{
	public static void main(String args[]){
		try{		
			//Setup call to server
			TTransport serverTransport = new TSocket(args[0], Integer.parseInt(args[1]));
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
			Server.Client serverClient = new Server.Client(serverProtocol);
			
			//Try to connect
			serverTransport.open();
			
			Scanner sc = new Scanner(System.in);
			System.out.print("Enter the filename for the sorting job: ");
			String inputFile = sc.nextLine();
			
			String outputFile = serverClient.submitJob(inputFile);
			
			System.out.println("Merge sort done! Output file is: " + outputFile);
			
			serverTransport.close();
			
		}
		catch(Exception e){}
	}
}