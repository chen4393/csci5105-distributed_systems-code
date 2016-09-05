/*
* Custom class to describe a compute node
*/

public class ComputeNodeInfo {
	private String IP;
	private int port;
		
	public ComputeNodeInfo(String IP, int port) {	
		this.IP = IP;
		this.port = port;
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
	
	public String toString(){
		return this.IP + ":" + this.port;
	}
}