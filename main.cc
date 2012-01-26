
#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;

	File tempFile;
	Page outPage, inPage;
	Record outRecord;
	int doesThePageHaveSpace = TRUE, pageNum = 0;

	// create a temporary file
	tempFile.Open(0, "aakashTempTest.bin");

        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison)) {
                	// temp.Print (&mySchema);
			// load records into a page until the records are empty
			// or the page gets full

			doesThePageHaveSpace = outPage.Append(&temp);

			//check the count of records
			//cout << outPage.GetRecNum() << endl;

			// if the page becomes full, write to file
			if (FALSE == doesThePageHaveSpace) {

				cout << "Page Full" << endl;

				//write to a temp file
				tempFile.AddPage(&outPage, pageNum);

				//read from a temp file
				tempFile.GetPage(&inPage, pageNum);

				//display all records
				while (TRUE == inPage.GetFirst(&outRecord)) {
					outRecord.Print(&mySchema);
				}

				// empty pages
				outPage.EmptyItOut();
			}
		}
        }

	// if the page was not full, empty it here
	if (TRUE == doesThePageHaveSpace) {
		
		cout << "Page has space!" << endl;

		//write to a temp file
		tempFile.AddPage(&outPage, pageNum);

		//read from a temp file
		tempFile.GetPage(&inPage, pageNum);

		//display all records
		while (TRUE == inPage.GetFirst(&outRecord)) {
			outRecord.Print(&mySchema);
		}

		// empty pages
		outPage.EmptyItOut();
	}

	(void)tempFile.Close();
	(void)tempFile.Remove("aakashTempTest.bin");

}
