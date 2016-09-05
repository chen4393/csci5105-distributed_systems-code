service Server {
	bool ping(),
	string submitJob(1: string inputFile),
	void join(1: string IP, 2: i32 port),
	void done(1: string intermediateFile, 2: i32 taskNumber, 3: i32 type)
}