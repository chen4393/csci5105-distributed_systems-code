service ComputeNode {
	bool ping(),	
	void startSort(1: string inputFile, 2: i64 offset, 3: i64 size, 4: i32 taskNumber),
	void startMerge(1: list<string> fileList, 2: i32 taskNumber),
	void printStatistics()
}