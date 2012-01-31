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

extern struct AndList *final;

DBFile::DBFile () {
	/* Initialize all class variables here */
	dbFile = NULL;
	curPage = NULL;
	curPageNum = 0;
}

DBFile::~DBFile () {
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

File * DBFile :: GetFileDetails() {
	return dbFile;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	/* For the first part of the project startup would be NULL,
	it would be defined later */
	/* Also, f_type should be heap only (refer enum fType) */
	if (NULL == f_path) {
		cout << "Invalid file name";
	}

	if (NULL != startup) {
		cout << "Expected pointer startup to be NULL";
	}

	if (heap != f_type) {
		cout << "Expected f_type to be heap";
	}

	/* Open the bin file and store the file descriptor */
	dbFile = new File();
	
	/* We would want to create a new file and hence lengthOfFile should be 0 */
	int lengthOfFile = 0;
	dbFile->Open(lengthOfFile, f_path);

	return TRUE;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
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

	Record recordBuffer;
	Page pageBuffer;
	int pageHasSpace = FALSE;
	int pagesInDBFile;
	int pageNumber;
	
	// check the number of pages inside the file
	pagesInDBFile = dbFile->GetLength();
	
	if (pagesInDBFile > 1) {
		pageNumber = (pagesInDBFile - 1);
	}
	else {
		pageNumber = 0;
	}

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

int DBFile::Open (char *f_path) {
	if (NULL == f_path) {
		cout << "Invalid file name";
		return FALSE;
	}

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
}

void DBFile::MoveFirst () {
	// move to the first record of the 1st page
	curPageNum = 0;
	dbFile->GetPage(curPage, curPageNum);
	curPage->GotoFirst();
}

int DBFile::Close () {
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

void DBFile::Add (Record &rec) {
	// get the last page
	int addToPageNum = (dbFile->GetLength() - 1);
	Page pageBuffer, newPage;

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
	if (FALSE == newPage.Append(&rec)) {
		cout << "New page, still cannot add record!" << endl;
		exit(1);
	}

	// as no pages are present, add to page 0
	dbFile->AddPage(&newPage, addToPageNum);
}

int DBFile::GetNext (Record &fetchme) {
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
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
