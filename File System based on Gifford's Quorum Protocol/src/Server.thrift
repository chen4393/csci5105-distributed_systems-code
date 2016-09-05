service Server {
	bool ping(),
	string write(1: string filename, 2: string contents),
	void writeAux(1: string filename, 2: string contents),	
	string read(1: string filename),
	string readAux(1: string filename),
	i32 getVersion(1: string filename),
	void updateVersion(1: string filename, 2: i32 version),
	map<string, i32> getVersionMap()
}