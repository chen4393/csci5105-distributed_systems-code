service Node {
	bool ping(),
	void write(1: string filename, 2: string contents),
	void writeAux(1: string filename, 2: string contents, 3: list<i32> visitedNodes),
	string read(1: string filename),
	string readAux(1: string filename, 2: list<i32> visitedNodes),
	void updateDHT(1: string nodesList),
	string getNodeDetails(),
	string getLogs(),
	void cleanup()	
}