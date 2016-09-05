

/**
* Custom class used to represent a Node in the system. The attributes used to identify the node are its IP, port number
* and the ID assigned to it by the SuperNode
*/

public class NodeInfo implements Comparable<NodeInfo>{
	private String IP;
	private String port;
	private int ID;
		
	public String getPort() {
		return port;
	}

	public int getID() {
		return ID;
	}

	public NodeInfo(String IP, String port, int ID){
		this.IP = IP;
		this.port = port;
		this.ID = ID;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public void setID(int iD) {
		ID = iD;
	}

	public String getIP() {
		return this.IP;
	}

	public void setIP(String IP) {
		this.IP = IP;
	}	
	
	public String toString(){
		return IP + ":" + port + ":" + ID;
	}

	/**
	* Method to compare two Nodes based on their ID
	*/
	@Override
	public int compareTo(NodeInfo other) {
		// TODO Auto-generated method stub
		if (this.ID < other.ID)
			return -1;
		else if (this.ID > other.ID)
			return 1;
		return 0;
	}}