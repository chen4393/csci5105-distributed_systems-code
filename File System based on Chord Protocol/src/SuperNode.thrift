service SuperNode {
	bool ping(),
	string join(1:string IP, 2:string Port),
	void postJoin(1: string IP, 2: string Port),
	string getNode(),
	string getLogs(),
	void exitSystem()
}

