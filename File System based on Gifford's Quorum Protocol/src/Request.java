/**
 * Class that represents a requst sent by a client to one of the servers
 * IP and Port are of the server that received the request
 * id is a distinct number assigned by the coordinator 
 * task represents the nature of operation
 */

public class Request {
	private String IP;
	private int port;
	private int id;
	
	// Task is 0 if read and 1 if write
	private int task = 0;
	
	public Request(String IP, int port, int task, int id){
		this.IP = IP;
		this.port = port;
		this.task = task;
		this.id = id;
	}
	
	//Getters and Setter follow

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getIP() {
		return IP;
	}

	public void setIP(String iP) {
		IP = iP;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getTask() {
		return task;
	}

	public void setTask(int task) {
		this.task = task;
	}
		
	@Override
	//Method to compare two request objects field by field
	public boolean equals(Object obj) {		
		Request other = (Request) obj;
		return this.IP.equals(other.IP) && this.port == other.port && this.task == other.task;
	}
	
	//Custom toString 
	public String toString(){
		return IP + ":" + port + "-" + task + "-" + id;
	}
}
