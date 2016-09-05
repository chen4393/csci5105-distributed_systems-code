service Coordinator {
	bool ping(),
	void join(1: string IP, 2: string port),
	string assembleQuorum(1: string IP, 2: string port, 3: i32 task),
	void done(1: string IP, 2: string port, 3: i32 task, 4: i32 id),
	void synch()	
}