
/**
Class which describes a sorting task that is received by a compute node
*
*/

public class SortTask{
	private String filename;
	private long offset;
	private long size;
	private int taskNumber;
	public SortTask(String filename, long offset, long size, int taskNumber) {
		super();
		this.filename = filename;
		this.offset = offset;
		this.size = size;
		this.taskNumber = taskNumber;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public int getTaskNumber() {
		return taskNumber;
	}
	public void setTaskNumber(int taskNumber) {
		this.taskNumber = taskNumber;
	}
	
	@Override
	public int hashCode(){
		return taskNumber;
	}
	
	//Override the equals
	@Override
	public boolean equals(Object other){
		SortTask temp = (SortTask) other;
		if (this.getTaskNumber() == temp.getTaskNumber())
			return true;
		return false;
	}
	
	public String toString(){
		return taskNumber + "";
	}
	
}
