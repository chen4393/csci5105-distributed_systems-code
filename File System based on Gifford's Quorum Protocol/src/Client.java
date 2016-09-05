import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
* This class connects to a server in the system and issues read/write requests
*/

public class Client{
	public static void main(String args[]){		
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Please enter the number of clients you want to run: ");
			int numberOfClients = Integer.parseInt(br.readLine());
		
			ArrayList<String> IPMap = new ArrayList<>();
			ArrayList<Integer> portMap = new ArrayList<>();
			ArrayList<Integer> choices = new ArrayList<>();
			ArrayList<String> filenameMap = new ArrayList<>();
			ArrayList<Integer> requestsMap = new ArrayList<>();
			
			for (int i=0;i<numberOfClients;i++){
				System.out.println("Enter the IP and port of the file server that client " + (i+1) + " wants to connect to: ");
				System.out.println("Please enter the IP and port on separate lines!");
				String IP = br.readLine();
				IPMap.add(IP);
				int port = Integer.parseInt(br.readLine());
				portMap.add(port);
				
				System.out.println("Enter 0 for read and 1 for write");
				int choice = Integer.parseInt(br.readLine());
				choices.add(choice);
				
				if (choice == 0){
					System.out.println("Enter filename to read: ");
				}
				else if (choice == 1){
					System.out.println("Enter filename to write: ");
				}
				
				String filename = br.readLine();
				filenameMap.add(filename);
				
				System.out.println("Finally, enter the number of such requests you want to send to the server: ");
				int numberOfRequests = Integer.parseInt(br.readLine());
				requestsMap.add(numberOfRequests);
			}
			
			ThreadedClient[] clients = new ThreadedClient[numberOfClients];
			
			for (int i=0;i<numberOfClients;i++){ 
				clients[i] = new ThreadedClient(IPMap.get(i), portMap.get(i), choices.get(i), filenameMap.get(i), requestsMap.get(i));
			}
			
			for (int i=0;i<numberOfClients;i++){ 
				clients[i].start();
			}
		}
		catch (Exception e){ }
	}
}