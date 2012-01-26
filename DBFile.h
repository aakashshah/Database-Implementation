#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

using namespace std;

class DBFile {

private:
	// store the File object pointer associated with DBFile
	File *dbFile;

	// pointer to the current page
	Page *curPage;

	// tells us the current page number that the file is at
	off_t curPageNum;

public:
	DBFile ();

	~DBFile();

	// returns the file pointer
	File * GetFileDetails ();

	// create a new DBFile whose path is given by fpath
	int Create (char *fpath, fType file_type, void *startup);

	// we should already have the path descriptor as the file is
	// already created. refer the object pointer dbFile to get details
	// of the opened file
	int Open (char *fpath);

	// close the DBFile. Do not delete it. Should release the
	// class variable dbFile.
	int Close ();

	// from the file present at loadpath, using the schema
	// given by myschema, copy all records from loadpath to DBFile (dbFile)
	void Load (Schema &myschema, char *loadpath);

	// move to the first record in the dbFile
	void MoveFirst ();

	// add a record to the end of the dbFile
	void Add (Record &addme);

	// get the next record in the dbFile blindly into fetchme
	int GetNext (Record &fetchme);

	// get the next record in the dbFile which satisfies the CNF
	// into fetchme
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
