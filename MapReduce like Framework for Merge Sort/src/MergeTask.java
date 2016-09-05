import java.util.List;

/**
* Custom class that describes the merging task received by a compute node from the server
* parameters are the files to merge and the task number
*/

public class MergeTask {
	private List<String> fileList;
	private int taskNumber;
	public MergeTask(List<String> fileList, int taskNumber) {
		super();
		this.fileList = fileList;
		this.taskNumber = taskNumber;
	}
	public List<String> getFileList() {
		return fileList;
	}
	public void setFileList(List<String> fileList) {
		this.fileList = fileList;
	}
	public int getTaskNumber() {
		return taskNumber;
	}
	public void setTaskNumber(int taskNumber) {
		this.taskNumber = taskNumber;
	}
	
	public int hashCode(){
		return taskNumber;
	}
	
	@Override
	public boolean equals(Object other){
		MergeTask temp = (MergeTask) other;
		if (this.getTaskNumber() == temp.getTaskNumber())
			return true;
		return false;
	}
	
	public String toString(){
		return taskNumber + "";
	}
}
