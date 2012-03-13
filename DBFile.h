#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree, invalid} fType;

typedef enum sortMode_e {reading, writing, invalidMode} sortMode;

typedef struct SortInfo_S { 
	OrderMaker *myOrder; 
	int runLength; 
}SortInfo;

#define DEFAULT_PIPE_BUFFER_LEN 100

using namespace std;

class GenericDBFile {
protected:
	// store the File object pointer associated with DBFile
        File *dbFile;

        // pointer to the current page
        Page *curPage;

        // tells us the current page number that the file is at
        off_t curPageNum;
public:
	GenericDBFile();
	virtual ~GenericDBFile();

	// returns the file pointer
        virtual File * GetFileDetails () = 0;

        // create a new DBFile whose path is given by fpath
        virtual int Create (char *fpath, fType file_type, void *startup) = 0;

        // we should already have the path descriptor as the file is
        // already created. refer the object pointer dbFile to get details
        // of the opened file
        virtual int Open (char *fpath) = 0;

        // close the DBFile. Do not delete it. Should release the
        // class variable dbFile.
        virtual int Close () = 0;

        // from the file present at loadpath, using the schema
        // given by myschema, copy all records from loadpath to DBFile (dbFile)
        virtual void Load (Schema &myschema, char *loadpath) = 0;

        // move to the first record in the dbFile
        virtual void MoveFirst () = 0;

        // add a record to the end of the dbFile
        virtual void Add (Record &addme) = 0;

        // get the next record in the dbFile blindly into fetchme
        virtual int GetNext (Record &fetchme) = 0;

        // get the next record in the dbFile which satisfies the CNF
        // into fetchme
        virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class Heap : public GenericDBFile {

public:	
	Heap();

        ~Heap();

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

class Sorted : public GenericDBFile {

private:
	// variables needed for sorting
	int runLen;
	OrderMaker fileOrder;

	// the current mode of the file (reading / writing)
	sortMode fileMode;

	// stores the name of the current file
	// gets updated when Create() or Open() is called
	char fname[MAX_FILENAME_LEN];

	// useful to keep the file sorted
	Pipe *in;
	Pipe *out;
	BigQ *bigQ;

	// functions usefil for keeping the file sorted
	bool InitOrUseBigQ();
	void Merge();

public:
	Sorted();

        ~Sorted();

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

class DBFile {

private:
	fType file_type;
	GenericDBFile *dbFile;
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
