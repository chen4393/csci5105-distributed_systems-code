

/**
* This enum defines a constant here which is the maximum number of nodes in the system.
*/

public enum Constants {
	
	MAX_NODES (16);
		
	private int value;
	private Constants(int value){
		this.value = value;
	}
	
	public int getValue(){
		return value;
	}
}
