#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>

extern struct AndList *final;

///////////////////////////////////////////////////////////////////////
/////////////////////// ABSTRACT BASE CLASS ///////////////////////////
///////////////////////////////////////////////////////////////////////

GenericDBFile :: GenericDBFile() {
	/* Initialize all class variables here */
	dbFile = NULL;
	curPage = NULL;
	curPageNum = 0;
}

GenericDBFile :: ~GenericDBFile() {
	if (NULL != dbFile) {
		dbFile->Close();
		delete dbFile;
	}
	dbFile = NULL;

	if (NULL != curPage) {
		delete curPage;
	}

	curPageNum = 0;
}

///////////////////////////////////////////////////////////////////////
/////////////////////////////// HEAP //////////////////////////////////
///////////////////////////////////////////////////////////////////////

Heap::Heap () {
}

Heap::~Heap () {
}

File * Heap :: GetFileDetails() {
	return dbFile;
}

int Heap::Create (char *f_path, fType f_type, void *startup) {

	if (heap != f_type) {
		cout << "Expected f_type to be heap" << endl;
		return FALSE;
	}

	/* Open the bin file and store the file descriptor */
	dbFile = new File();
	
	/* We would want to create a new file and hence lengthOfFile should be 0 */
	int lengthOfFile = 0;
	dbFile->Open(lengthOfFile, f_path);

	// take care of the page pointer
	if (NULL == curPage) {
		curPage = new Page();
		curPageNum = 0;
	}
	
	// create a meta file which tells about the file being created
	char metaFileName[MAX_FILENAME_LEN] = {0};
	sprintf(metaFileName, "%s.meta", f_path);
	FILE *metaFile = fopen(metaFileName, "w");
	if (!metaFile) {
		cout << "Failed to create sort meta file!" << endl;
		dbFile->Close();
		return FALSE;
	}

	// the first line tells about the type of file
	fprintf(metaFile, "HEAP\n");
	fclose(metaFile);

	return TRUE;
}

void Heap::Load (Schema &f_schema, char *loadpath) {
	/* copy all files from .tbl file (loadpath) into dbFile */
	if (NULL == loadpath) {
		cout << "Invalid file name!"<< endl;
		return;
	}

	if (NULL == dbFile) {
		cout << "FATAL: Expected the file to be open!";
		return;
	}

	// open the file in read mode from which the data will be copied
	FILE *tblFile = fopen(loadpath, "r");
	if (!tblFile) {
		cout << "Failed to open file at " << loadpath << endl;
		return;
	}

	Record recordBuffer;
	Page pageBuffer;
	int pageHasSpace = FALSE;
	int pageNumber = dbFile->LastUsedPageNum();
	
	// get all records from the .tbl (load) file
	while (TRUE == recordBuffer.SuckNextRecord(&f_schema, tblFile)) {
		// populate the records into pages and dump the pages
		// into files.
		pageHasSpace = pageBuffer.Append(&recordBuffer);
		if (FALSE == pageHasSpace) {
			// page is full, dump the page into the file
			dbFile->AddPage(&pageBuffer, pageNumber);
			pageNumber++;

			// empty the page so that more records can fit in
			pageBuffer.EmptyItOut();

			// the record which we were not able to add, add now
			if (FALSE == pageBuffer.Append(&recordBuffer)) {
				cout << "Error in adding record to an empty page!" << endl;
				exit (1);
			}
		}
	}

	// if the pageHasSpace variable is set, it means the page still has data
	// empty that remaining data into the file
	if (TRUE == pageHasSpace) {
		dbFile->AddPage(&pageBuffer, pageNumber);
		pageNumber++;
		pageBuffer.EmptyItOut();
	}

	fclose(tblFile);
}

int Heap::Open (char *f_path) {
	if (NULL == f_path) {
		cout << "Invalid file name";
		return FALSE;
	}

	// create a new file
	if (NULL == dbFile) {
		dbFile = new File();
	}

	/* We should not be creating a new file here,
	hence lengthOfFile should NOT be 0 */
	int lengthOfFile = 1;
	dbFile->Open(lengthOfFile, f_path);

	// take care of the page pointer
	if (NULL == curPage) {
		curPage = new Page();
		curPageNum = 0;
	}

	return TRUE;
}

void Heap::MoveFirst () {
	// move to the first record of the 1st page
	curPageNum = 0;
	dbFile->GetPage(curPage, curPageNum);
	//curPage->GotoFirst();
}

int Heap::Close () {
	/* Close only if the file pointer is valid */
	if (NULL != dbFile) {
		
		(void)dbFile->Close();
		delete dbFile;
		dbFile = NULL;
	}

	if (NULL != curPage) {
		delete curPage;
		curPage = NULL;
	}

	curPageNum = 0;

	return TRUE;
}

void Heap::Add (Record &rec) {
	// get the last page
	int addToPageNum = (dbFile->GetLength() - 2);
	Page pageBuffer, newPage;

	pageBuffer.EmptyItOut();

	// there should be atleast 1 page in the file,
	// if not create one ( < 0 => no pages present!)
	if (addToPageNum < 0) {
		if (FALSE == pageBuffer.Append(&rec)) {
			cout << "No pages, still "
				"cannot add record!" << endl;
			exit(1);
		}

		// as no pages are present, add to page 0
		dbFile->AddPage(&pageBuffer, 0);
		return;
	}

	// check if it has space, if it has, add record
	// to that page
	dbFile->GetPage(&pageBuffer, addToPageNum);
	if (FALSE != pageBuffer.Append(&rec)) {
		dbFile->AddPage(&pageBuffer, addToPageNum);
		return;
	}

	// DO NOT ADD THE NEW RECORD INTO 'pageBuffer'
	// IT STILL HAS THE LAST PAGE OF THE FILE.
	// ADD TO THE NEW PAGE ONLY. I DO NOT WANT
	// TO CALL THE EXPENSIVE 'EmptyItOut()'
	// FUNCTION TO USE THE SAME 'pageBuffer' VARIABLE

	// if it does not have space, create a new page
	// and add the record there
	addToPageNum++;
	newPage.EmptyItOut();
	if (FALSE == newPage.Append(&rec)) {
		cout << "New page, still cannot add record!" << endl;
		exit(1);
	}

	// as current pages are full, add to new page
	dbFile->AddPage(&newPage, addToPageNum);
}

int Heap::GetNext (Record &fetchme) {
	// get the reocrds pointed by the current record
	bool gotRecord = false;

	while (!gotRecord) {

		// as the first page is always empty,
		// 0 translates to page 1 (min curLength needed = 2)
		// 1 translates to page 2 (min curLength needed = 2)
		if ((curPageNum+1) >= dbFile->GetLength()) {
			// you do not have any data (no more pages)
			return FALSE;
		}

		// check if we actually got any records
		if (0 != curPage->GetFirst(&fetchme)) {
			gotRecord = true;
		}
		else {
			//if the current page does not have
			//any records, move to the next page
			curPageNum++;
			if ((curPageNum+1) >= dbFile->GetLength()) {
				// you do not have any data (no more pages)
				return FALSE;
			}
			dbFile->GetPage(curPage, curPageNum);
		}
	}

	return TRUE;
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	// get the record pointed by the current record which satisfies
	// the cnf passed on by the function
	ComparisonEngine comp;

	// GetNext returns all records whether they satisfy the CNF or not
	while (GetNext(fetchme)) {
		// apply cnf and filter out incorrect records
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return TRUE;
		}
	}

	return FALSE;
}

///////////////////////////////////////////////////////////////////////
////////////////////////////// SORTED /////////////////////////////////
///////////////////////////////////////////////////////////////////////
Sorted :: Sorted() {
	bigQ = NULL;
	in = NULL;
	out = NULL;
}

Sorted :: ~Sorted() {
	// release memory if allocated
	fileMode = invalidMode;
	fname[0] = '\0';

	if (NULL != bigQ) {
		delete bigQ;
		bigQ = NULL;
	}

	if (NULL != in) {
		delete in;
		in = NULL;
	}

	if (NULL != out) {
		delete out;
		out = NULL;
	}
}

File* Sorted :: GetFileDetails() {
	return dbFile;
}

bool Sorted :: InitOrUseBigQ() {

	// if the file is reading, initialize the pipes and the bigQ, so
	// that the function calling it should be able to use the in pipe.
	// the caller function would keep on inserting data into the in pipe
	// finally, when any function other than Load or Add is called, the BigQ
	// will be merged with the exisitng file!
	if (reading == fileMode) {
                if ((NULL != in) || (NULL != out) || (NULL != bigQ)) {
                        cout << "Impossible! Either of in, out or bigQ is valid in reading mode!" << endl;
                        return false;
                }

                in = new Pipe(DEFAULT_PIPE_BUFFER_LEN);
                if (NULL == in) {
                        cout << "Failed to allocate memory!" << endl;
                        return false;
                }

                out = new Pipe(DEFAULT_PIPE_BUFFER_LEN);
                if (NULL == out) {
                        cout << "Failed to allocate memory!" << endl;
                        delete in;
                        return false;
                }

                bigQ = new BigQ((*in), (*out), fileOrder, runLen);
                if (NULL == bigQ) {
                        cout << "Failed to allocate memory!" << endl;
                        delete in;
                        delete out;
                        return false;
                }

                fileMode = writing;

	// if the mode is already writing, the bigQ is ready to use, just insert
	// data into the in pipe!
        } else if (writing == fileMode) {
                if (NULL == bigQ) {
                        cout << "Impossible! mode is writing and bigQ is null!" << endl;
                        return false;
		}
        } else {
                cout << "Invalid mode encountered!" << endl;
                return false;
        }

	return true;
}

void Sorted :: Merge() {

	// close input pipe so that the BigQ class can start sorting
	in->ShutDown();

	// compare the records from the out pipe and the records from file
	// merge them and write them onto a new temporary file
	int fileHasData = TRUE;
	int pipeHasData = TRUE;

	// this file would be the resultant merge file
	DBFile tmpFile;
	Record fileRec, pipeRec;

	// used to compare 2 records
	ComparisonEngine cEng;
	int compResult;

	char tmpFileName[MAX_FILENAME_LEN];// = "askiu3o3iurnmnsdf.tmp\0";
	sprintf(tmpFileName, "%u%u.tmp\0", (unsigned int)getpid(), (unsigned int)pthread_self());
	char tmpMetaFileName[MAX_FILENAME_LEN];// = "askiu3o3iurnmnsdf.tmp.meta\0";
	sprintf(tmpMetaFileName, "%s.meta\0", tmpFileName);

	tmpFile.Create(tmpFileName, heap, NULL);

	// we would merge the pipe and the current file and hence, the current
	// file should be moved to the first record
	this->MoveFirst();

	// index 0 belongs to dbFile while index 1 belongs to out pipe
	fileHasData = this->GetNext(fileRec);
	pipeHasData = out->Remove(&pipeRec);

	while (fileHasData && pipeHasData) {
		//compare and write it to temp file
		compResult = cEng.Compare(&fileRec, &pipeRec, &fileOrder);
		if (1 == compResult) {
			// write pipeRec in the out page
			tmpFile.Add(pipeRec);

			// get another one from pipe
			pipeHasData = out->Remove(&pipeRec);
		} else {
			// write fileRec in the out page
			tmpFile.Add(fileRec);

			// get another one from file
			fileHasData = this->GetNext(fileRec);
		}
	}

	// either of them should have data left,
	// insert everything into the file
	while (fileHasData) {
		tmpFile.Add(fileRec);
		fileHasData = this->GetNext(fileRec);
	}

	while (pipeHasData) {
		tmpFile.Add(pipeRec);
		pipeHasData = out->Remove(&pipeRec);
	}

	// copy the temporary file into the original file!
	// as the temporary file will always be greater or equal
	// to the original file, the original file is guaranteed to be
	// overwritten completely.
	this->Close();

	tmpFile.Close();

	// remove fname and now rename tmpFileName to fname
	(void)remove(fname);
	if (0 != rename(tmpFileName, fname)) {
		cout << "Failed to rename " << tmpFileName << " to " << fname << endl;
		remove(tmpFileName);
		remove(tmpMetaFileName);
		return;
	}


	// remove the meta file!
	remove(tmpMetaFileName);

	// delete bigQ and the pipes as they are no longer in use
	delete in;
	in = NULL;
	delete out;
	out = NULL;
	delete bigQ;
	bigQ = NULL;

	return;
}

int Sorted :: Create (char *f_path, fType f_type, void *startup) {

	// basic validation
	if (!f_path) {
		cout << "Empty file name!" << endl;
		return FALSE;
	}

	if (sorted != f_type) {
		cout << "Expected f_type to be sorted" << endl;
		return FALSE;
	}

	if (NULL == startup) {
		cout << "Expected startup parameters for sorted file!" << endl;
		return FALSE;
	}

	// save the file name for future use
	if (fname[0] == '\0') {
		strcpy(fname, f_path);
	}

	// Open the bin file and store the file descriptor
	dbFile = new File();
	if (NULL == dbFile) {
		cout << "Failed to create dbFile!" << endl;
		return FALSE;
	}

	// We would want to create a new file and hence lengthOfFile should be 0 
	int lengthOfFile = 0;
	dbFile->Open(lengthOfFile, f_path);

	// take care of the page pointer
	if (NULL == curPage) {
		curPage = new Page();
		if (NULL == curPage) {
			cout << "Failed to create page!" << endl;
			dbFile->Close();
			return FALSE;
		}
	}
	curPageNum = 0;
	
	// create a meta file which tells about the file being created
	char metaFileName[MAX_FILENAME_LEN] = {0};
	sprintf(metaFileName, "%s.meta", f_path);
	FILE *metaFile = fopen(metaFileName, "w");
	if (!metaFile) {
		cout << "Failed to create sort meta file!" << endl;
		dbFile->Close();
		return FALSE;
	}

	// the first line tells about the type of file
	fprintf(metaFile, "SORTED\n");

	// then the details about the file
	SortInfo *sortInfo = (SortInfo *)startup;
	fprintf(metaFile, "%d\n", sortInfo->runLength);
	sortInfo->myOrder->DumpData(metaFile);
	fclose(metaFile);
	return TRUE;
}

void Sorted :: Load (Schema &f_schema, char *loadpath) {

	// copy all files from .tbl file (loadpath) into the input pipe!
	if (NULL == loadpath) {
		cout << "Invalid file name!"<< endl;
		return;
	}

	if (NULL == dbFile) {
		cout << "FATAL: Expected the file to be open!";
		return;
	}

	// check the current mode, if writing, insert the record into
	// the input pipe of BigQ. if current mode is reading, change the mode
	// to writing, merge the BigQ with the sorted file and write a new file

	if (!InitOrUseBigQ()) {
		cout << "Failed to initialize or use BigQ!" << endl;
		return;
	}

	// open the file in read mode from which the data will be copied
	FILE *tblFile = fopen(loadpath, "r");
	if (!tblFile) {
		cout << "Failed to open file at " << loadpath << endl;
		return;
	}

	Record recordBuffer;
	
	// get all records from the .tbl (load) file
	while (TRUE == recordBuffer.SuckNextRecord(&f_schema, tblFile)) {
		// keep on inserting them into the input pipe!
		in->Insert(&recordBuffer);
	}

	fclose(tblFile);
}

int Sorted :: Open (char *f_path) {
	if (NULL == f_path) {
		cout << "Invalid file name";
		return FALSE;
	}

	// create a new file
	if (NULL == dbFile) {
		dbFile = new File();
	}

	// save the file name for future use
	strcpy(fname, f_path);

	/* We should not be creating a new file here,
	hence lengthOfFile should NOT be 0 */
	int lengthOfFile = 1;
	dbFile->Open(lengthOfFile, f_path);

	// take care of the page pointer
	if (NULL == curPage) {
		curPage = new Page();
	}
	curPageNum = 0;

	// get the run length and the order of the sorted file
	char line[64];
	char metaFileName[MAX_FILENAME_LEN] = {0};
	sprintf(metaFileName, "%s.meta", f_path);
	FILE *metaFile = fopen(metaFileName, "r");
	if (!metaFile) {
		return FALSE;
	}

	// type of file
	if (EOF == fscanf(metaFile, "%s", line)) {
		fclose(metaFile);
		return FALSE;
	}

	// run length
	if (EOF == fscanf(metaFile, "%d", &runLen)) {
		fclose(metaFile);
		return FALSE;
	}

	// read the ordermaker data from the meta file	
	fileOrder.ReadData(metaFile);

	fclose(metaFile);

	// switch the mode of file to reading
	fileMode = reading;

	return TRUE;
}

void Sorted :: MoveFirst () {

	// check if the file is in writing mode, if yes,
        // merge the records from the file and the pipe!
        if (writing == fileMode) {
                fileMode = reading;
                Merge();
        }

	// move to the first record of the 1st page
	curPageNum = 0;
	if (!dbFile->IsFileEmpty()) {
		dbFile->GetPage(curPage, curPageNum);
	}
}

int Sorted :: Close () {

	// check if the file is in writing mode, if yes,
	// merge the records from the file and the pipe!
	if (writing == fileMode) {
		fileMode = reading;
		Merge();
	}

	/* Close only if the file pointer is valid */
	if (NULL != dbFile) {
		
		(void)dbFile->Close();
		delete dbFile;
		dbFile = NULL;
	}

	if (NULL != curPage) {
		delete curPage;
		curPage = NULL;
	}

	curPageNum = 0;

	return TRUE;
}

void Sorted :: Add (Record &rec) {

	// check the current mode, if writing, insert the record into
	// the input pipe of BigQ. if current mode is reading, change the mode
	// to writing, merge the BigQ with the sorted file and write a new file

	if (!InitOrUseBigQ()) {
		cout << "Failed to initialize or use BigQ!" << endl;
		return;
	}

	in->Insert(&rec);
}

int Sorted :: GetNext (Record &fetchme) {

	// check if the file is in writing mode, if yes,
        // merge the records from the file and the pipe!
        if (writing == fileMode) {
                fileMode = reading;
                Merge();
        }

	// get the reocrds pointed by the current record
	bool gotRecord = false;

	while (!gotRecord) {

		// as the first page is always empty,
		// 0 translates to page 1 (min curLength needed = 2)
		// 1 translates to page 2 (min curLength needed = 2)
		if ((curPageNum+1) >= dbFile->GetLength()) {
			// you do not have any data (no more pages)
			return FALSE;
		}

		// check if we actually got any records
		if (0 != curPage->GetFirst(&fetchme)) {
			gotRecord = true;
		}
		else {
			//if the current page does not have
			//any records, move to the next page
			curPageNum++;
			if ((curPageNum+1) >= dbFile->GetLength()) {
				// you do not have any data (no more pages)
				return FALSE;
			}
			dbFile->GetPage(curPage, curPageNum);
		}
	}

	return TRUE;
}

int Sorted :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	// check if the file is in writing mode, if yes,
        // merge the records from the file and the pipe!
        if (writing == fileMode) {
                fileMode = reading;
                Merge();
        }

	// check if the OrderMaker for the sorted file can be used to
	// GetNext the record by evaulating the CNF passed alongwith
	// if no, continue the linear scan and spit out results, if yes,
	// build a "query" OrderMaker based on the CNF that is passed
	OrderMaker cnfOrder, literalOrder;
	bool usable = cnf.GetSortOrders(fileOrder, cnfOrder, literalOrder);

	//cout << "CNF Order: " << endl;
	//cnfOrder.Print();
	//cout << "File Order: " << endl;
	//fileOrder.Print();

	// result gives the number of attributes in cnfOrder, if none,
	// scan sequentially
	//cout << "Result: " << usable << endl;
	if (false == usable) {
		// get the record pointed by the current record which satisfies
		// the cnf passed on by the function
		ComparisonEngine comp;

		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme)) {
			// apply cnf and filter out incorrect records
			if (comp.Compare(&fetchme, &literal, &cnf)) {
				return TRUE;
			}
		}

		return FALSE;
	}

	// perform binary search
	if (dbFile->IsFileEmpty()) {
		cout << "Trying to search on an empty sorted file!" << endl;
		return FALSE;
	}

	// we need to perform binary search from the current record
	int start = (((int)curPageNum)+1);
	int end = dbFile->LastUsedPageNum();
	int mid;
	int recFoundOnPageNum = (int)curPageNum;
	int initialPageNum = (int)curPageNum;
	bool scanCurrentPage = true;

	ComparisonEngine comp;

	Page pageBuffer;
	Record recBuffer;

	//cout << "Start: "<< start << " End: " << end << endl;

	// first we would check if the records in the current page should be
	// scanned or not, the idea is to check the first record on the next page
	// if that record is < than the CNF order, that means chances of finding
	// the records that may match the CNF order and in the second-half, and 
	// hence binary search should be performed. If its == or > that means
	// we should scan until we reach the next page
	if (start <= end) { // (= means that start is the last page and curpage is the second last)
		dbFile->GetPage(&pageBuffer, start);
		if (0 != pageBuffer.GetFirst(&recBuffer)) {
			// found a record, compare
			//recBuffer.Print(new Schema("catalog", "customer"));
			if ((-1) == comp.Compare(&recBuffer, &cnfOrder, &literal, &literalOrder)) {
				scanCurrentPage = false;
			}
		}
		else {
			cout << "Should have found record here!" << endl;
			return FALSE;
		}

		pageBuffer.EmptyItOut();
	}

	//cout << "Scan current page?: " << scanCurrentPage << endl;

	int result;
	if (scanCurrentPage) {
		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme))  {
			// apply cnf and filter out incorrect records
			//if (comp.Compare(&fetchme, &literal, &cnf)) {
			//	return TRUE;
			//}
			result = comp.Compare(&fetchme, &cnfOrder, &literal, &literalOrder);
			if (0 == result) {
				return TRUE;
			}
			// if result is 1 that means you have already crossed
			// all possible records
			else if (1 == result) {
				return FALSE;
			}
		}

		// if the record was not found on the current page, there are no records
		return FALSE;
	}

	// now as the record is not present in the current page, start binary search
	while (end >= start) {

		mid = start + ((end - start)/2);
		//cout << "Mid: " << mid << endl;

		// get the record from middle page and compare with the literal
		// record, based on the comparision result move either upward
		// or downward
		curPage->EmptyItOut();
		dbFile->GetPage(curPage, mid);
		curPageNum = mid;
		if (0 == curPage->GetFirst(&recBuffer)) {
			cout << "No records found on page " << mid  <<endl;
			return FALSE;
		}

		result = comp.Compare(&recBuffer, &cnfOrder, &literal, &literalOrder);
		//recBuffer.Print(new Schema("catalog", "customer"));
		if ((-1) == result) {
			start = mid + 1;
			if (start > end) {
				recFoundOnPageNum = end;
				break;
			}
		} else if (1 == result) {
			end = mid - 1;
			if (end < initialPageNum) {
				recFoundOnPageNum = initialPageNum;
				break;
			}
		} else {
			recFoundOnPageNum = mid;
			end = mid - 1;
		}
	}

	//cout << "S: " << start << " E: " << end << " M: " << mid << endl;

	// we need to scan the last page which we narrowed down
	if (start > end) {
		recFoundOnPageNum = (start - 1);
	}

	//cout << "Rec found on page num: " << recFoundOnPageNum << endl;

	// the inital page was already scanned, so if the record in found on some
	// other page, scan the entire page so see if you find any record on that
	// page, if not there are no more records which satisfy the CNF
	if (recFoundOnPageNum > initialPageNum) {
		curPage->EmptyItOut();
		dbFile->GetPage(curPage, recFoundOnPageNum);
		curPageNum = recFoundOnPageNum;

		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme)) {
			// apply cnf and filter out incorrect records
			//if (comp.Compare(&fetchme, &literal, &cnf)) {
			//	return TRUE;
			//}

			result = comp.Compare(&fetchme, &cnfOrder, &literal, &literalOrder);
			if (0 == result) {
				return TRUE;
			}
			// if result is 1 that means you have already crossed
			// all possible records
			else if (1 == result) {
				return FALSE;
			}
		}

		// if the record was not found on the current page, there are no records
		return FALSE;
	}

	return FALSE;
}


///////////////////////////////////////////////////////////////////////
////////////////////////////// DBFILE /////////////////////////////////
///////////////////////////////////////////////////////////////////////

DBFile :: DBFile () {
	file_type = invalid;
	dbFile = NULL;
}

DBFile :: ~DBFile () {
	if (NULL != dbFile) {
		dbFile->Close();
		delete dbFile;
		dbFile = NULL;
	}

	file_type = invalid;
}

File * DBFile :: GetFileDetails() {
	
	if (NULL == dbFile) {
		cout << "GetFileDetails: Please open the file first!" << endl;
		return NULL;
	}

	return dbFile->GetFileDetails();
}

int DBFile :: Create (char *f_path, fType f_type, void *startup) {
	/* For the first part of the project startup would be NULL,
	it would be defined later */
	/* Also, f_type should be heap only (refer enum fType) */
	if (NULL == f_path) {
		cout << "Invalid file name" << endl;
		return FALSE;
	}

	if (invalid == f_type) {
		cout << "Expected f_type to be either heap or sorted!" << endl;
		return FALSE;
	}

	switch (f_type) {
		case heap: {
			dbFile = new (std::nothrow) Heap;
			break;
		}
		case sorted: {
			dbFile = new (std::nothrow) Sorted;
			break;
		}
		default: {
			cout << "Unsupported file type requested! " << f_type << endl;
			return FALSE;
		}
	}

	if (!dbFile) {
		cout << "Failed to allocate sorted file!" << endl;
		return FALSE;
	}

	return dbFile->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
		
	if (NULL == dbFile) {
		cout << "Load: Please open the file first!" << endl;
		return;
	}

	dbFile->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {

	if (!f_path) {
		cout << "File name invalid!" << endl;
		return FALSE;
	}

	char line[64] = {0};
	char metaFileName[MAX_FILENAME_LEN] = {0};

	sprintf(metaFileName, "%s.meta", f_path);

	// determine the file type
	if (invalid == file_type) {
		// open meta file
		FILE *metaFile = fopen(metaFileName, "r");
		if (!metaFile) {
			cout << "Failed to determine file type for file " << f_path << endl;
			return FALSE;
		}

		if (EOF == fscanf(metaFile, "%s", line)) {
			cout << "Failed to determine file type!" << endl;
			return FALSE;
		}

		if ('H' == line[0]) {
			file_type = heap;
			if (NULL == dbFile) {
				dbFile = new Heap();
			}
		} else if ('S' == line[0]) {
			file_type = sorted;
			if (NULL == dbFile) {
				dbFile = new Sorted();
			}
		}

		fclose(metaFile);
	}

	if (NULL == dbFile) {
		cout << "Cannot determine file type!" << endl;
		return FALSE;
	}

	int fileOpened = dbFile->Open(f_path);
	if (TRUE == fileOpened) {
		dbFile->MoveFirst();
	}

	return fileOpened;
}

void DBFile::MoveFirst () {
	
	if (NULL == dbFile) {
		cout << "MoveFirst: Please open the file first!" << endl;
		return;
	}

	dbFile->MoveFirst();
}

int DBFile::Close () {

	if (NULL == dbFile) {
		cout << "Close: Please open the file first!" << endl;
		return FALSE;
	}

	return dbFile->Close();
}

void DBFile::Add (Record &rec) {

	if (NULL == dbFile) {
		cout << "Add: Please open the file first!" << endl;
		return;
	}

	dbFile->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	
	if (NULL == dbFile) {
		cout << "GetNext: Please open the file first!" << endl;
		return FALSE;
	}

	return dbFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	if (NULL == dbFile) {
		cout << "GetNext (CNF): Please open the file first!" << endl;
		return FALSE;
	}
	return dbFile->GetNext(fetchme, cnf, literal);
}
