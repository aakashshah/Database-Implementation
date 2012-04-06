#include "RelOp.h"
#include <string.h>

//////////////// ABSTRACT CLASS /////////////////
RelationalOp :: RelationalOp() {
	wArgs = NULL;
}

RelationalOp :: ~RelationalOp() {
	if (NULL != wArgs) {
		delete wArgs;
		wArgs = NULL;
	}
}

//////////////// SELECT FILE ///////////////////
void* workerSelectFile(void *args) {
	// read date from the file
	// apply CNF and if that is valid, copy the data to outPipe
	workerArgs *wArgs = (workerArgs *)args;

	wArgs->dbfile->MoveFirst();

	Record recBuffer;

	while(wArgs->dbfile->GetNext(recBuffer, *(wArgs->cnf), *(wArgs->literal))) {
		wArgs->out->Insert(&recBuffer);
	}

	wArgs->out->ShutDown();
}

void SelectFile :: Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

	wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
	wArgs->dbfile = &inFile;
	wArgs->in = NULL;
	wArgs->out = &outPipe;
	wArgs->cnf = &selOp;
	wArgs->literal = &literal;

	// a successfull creation of thread returns 0
	if (pthread_create(&workerThread, NULL, workerSelectFile, (void *)wArgs)) {
		cout << "Failed to create select file worker thread!" << endl;
	}
}

void SelectFile :: WaitUntilDone () {

	pthread_join (workerThread, NULL);
	
	if (NULL != wArgs) {
		delete wArgs;
		wArgs = NULL;
	}
}

void SelectFile :: Use_n_Pages (int runlen) {
	this->runlen = runlen;
}

//////////////// SELECT PIPE ///////////////////
void* workerSelectPipe(void *args) {
	// read the record from the input pipe
	// apply CNF and if that is valid, copy the data to outPipe
	workerArgs *wArgs = (workerArgs *)args;

	Record recBuffer;
	ComparisonEngine cEng;

	while(wArgs->in->Remove(&recBuffer)) {
		if (cEng.Compare(&recBuffer, wArgs->literal, wArgs->cnf)) {
			wArgs->out->Insert(&recBuffer);
		}
	}

	wArgs->out->ShutDown();
}

void SelectPipe :: Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
	wArgs->in = &inPipe;
	wArgs->out = &outPipe;
	wArgs->cnf = &selOp;
	wArgs->literal = &literal;

	// a successfull creation of thread returns 0
	if (pthread_create(&workerThread, NULL, workerSelectPipe, (void *)wArgs)) {
		cout << "Failed to create select pipe worker thread!" << endl;
	}
}

void SelectPipe :: WaitUntilDone () {

	pthread_join (workerThread, NULL);
	
	if (NULL != wArgs) {
		delete wArgs;
		wArgs = NULL;
	}
}

void SelectPipe :: Use_n_Pages (int runlen) {
	this->runlen = runlen;
}


/////////////////// PROJECT ///////////////////////

void* workerProject (void *args) {
	workerArgs *wArgs = (workerArgs *)args;

	Record recBuffer;

	while (wArgs->in->Remove(&recBuffer)) {
		//recBuffer.Print(new Schema("catalog", "part"));
		recBuffer.Project(wArgs->keepMe, wArgs->numAttsOutput, wArgs->numAttsInput);
		//recBuffer.Print(new Schema("tmpSchema", "part"));
		wArgs->out->Insert(&recBuffer);
	}

	wArgs->out->ShutDown();
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

	// read from input pipe
	// strip the attributes and place them into the out pipe
	wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
	wArgs->in = &inPipe;
	wArgs->out = &outPipe;
	wArgs->keepMe = keepMe;
	wArgs->numAttsInput = numAttsInput;
	wArgs->numAttsOutput = numAttsOutput;

	// a successfull creation of thread returns 0
	if (pthread_create(&workerThread, NULL, workerProject, (void *)wArgs)) {
		cout << "Failed to create project worker thread!" << endl;
	}
}

void Project :: WaitUntilDone() {

	pthread_join (workerThread, NULL);
	
	if (NULL != wArgs) {
		delete wArgs;
		wArgs = NULL;
	}
}

void Project :: Use_n_Pages (int runlen) {
	this->runlen = runlen;
}

/////////////////// SUM ///////////////////////

void* workerSum (void *args) {
	workerArgs *wArgs = (workerArgs *)args;

	Record *recBuffer = new Record;
	int iResult = 0, ires;
	double dResult = 0, dres;
	bool hasRecord = false;
	Type returned;

	while (wArgs->in->Remove(recBuffer)) {
		returned = wArgs->func->Apply(*recBuffer, ires, dres);

		if (Int == returned) {
			iResult += ires;
		} else if (Double == returned) {
			dResult += dres;
		} else {
			cout << "Warning!: Function returned an invalid 'Type'" << endl;
			delete recBuffer;
			wArgs->out->ShutDown();
			return NULL;
		}

		delete recBuffer;
		recBuffer = new Record;
		hasRecord = true;
	}

	// only if a sum is made, record should be given
	// into the outpipe
	if (hasRecord) {
		// compose record and insert into the outpipe
		char cSum[4];
		char cSchemaName[8];
		char space[128];

		sprintf(cSum, "sum\0");
		sprintf(cSchemaName, "sum_sch\0");
		Attribute att = {cSum, returned};
		Schema sumSchema (cSchemaName, 1, &att);

		if (Int == returned) {
			sprintf(space, "%d|\0", iResult);
		} else if (Double == returned) {
			sprintf(space, "%f|\0", dResult);
		}

		recBuffer->ComposeRecord(&sumSchema, space);
		wArgs->out->Insert(recBuffer);
	}
	
	delete recBuffer;
	
	wArgs->out->ShutDown();
}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {

	wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
	wArgs->in = &inPipe;
	wArgs->out = &outPipe;
	wArgs->func = &computeMe;

	// a successfull creation of thread returns 0
	if (pthread_create(&workerThread, NULL, workerSum, (void *)wArgs)) {
		cout << "Failed to create sum worker thread!" << endl;
	}
}

void Sum :: WaitUntilDone() {

	pthread_join (workerThread, NULL);
	
	if (NULL != wArgs) {
		delete wArgs;
		wArgs = NULL;
	}
}

void Sum :: Use_n_Pages (int runlen) {
	this->runlen = runlen;
}


///////////////////// JOIN ////////////////////////

int GetRecAtts(Record &rec) {
	return (((int *) rec.bits)[1]/(sizeof(int)) - 1);
}

void NestedLoopJoin(workerArgs *wArgs) {
	// sort both relations
	// dump the right relation while there is a match
	// for it on the left relation
	OrderMaker lOrder, rOrder;

	int result = wArgs->cnf->GetSortOrders(lOrder, rOrder);

	Pipe lOutPipe(100), rOutPipe(100);
	BigQ lBigQ(*(wArgs->in), lOutPipe, lOrder, wArgs->runlen);
	BigQ rBigQ(*(wArgs->inR), rOutPipe, rOrder, wArgs->runlen);

	// compare the 1st record of both relations
	// until left < right, skip left
	// until left > right, skip right
	// until left == right, dump right into a file
	DBFile tmpFile;
	char tmpFileName[MAX_FILENAME_LEN];
	char metaFileName[MAX_FILENAME_LEN];
	sprintf(tmpFileName, "%u%u.dump\0", (unsigned int)getpid(), (unsigned int)pthread_self());
	sprintf(metaFileName, "%s.meta\0", tmpFileName);
	Record *lRec = new Record, *rRec = new Record, *recBuffer = new Record;
	ComparisonEngine cEng;

	bool readLeft = false, readRight = false, dumpFileExists = false, skipCompare = true;

	if (!lOutPipe.Remove(lRec)) {
		wArgs->out->ShutDown();
		return;
	}

	if (!rOutPipe.Remove(rRec)) {
		wArgs->out->ShutDown();
		return;
	}

	// get the configuration of left and right records
	int lNumAtts = GetRecAtts(*lRec);
	int rNumAtts = GetRecAtts(*rRec);
	int attToKeep[lNumAtts+rNumAtts];
	for (int i = 0; i < lNumAtts; i++) {
		attToKeep[i] = i;
	}
	for (int i = 0; i < rNumAtts; i++) {
		attToKeep[i+lNumAtts] = i;
	}

	while (true) {
		// if need to read from left relalation
		if (readLeft) {
			if (!lOutPipe.Remove(lRec)) {
				break;
			}
			readLeft = false;
		}

		// if need to read from right relation
		if (readRight) {
			if (!rOutPipe.Remove(rRec)) {
				break;
			}
			readRight = false;
		}

		result = cEng.Compare(lRec, &lOrder, rRec, &rOrder);
		if (result < 0) {
			// left record is lagging behind
			// first check if the dump file exists and compare
			// the first record with the lRec, if a match, all
			// records in the dump file should be merged with
			// lRec otherwise, close and delete the dump file
			Record rightRec;
			if (dumpFileExists && !skipCompare) {
				//cout << "here 0" << endl; 
				tmpFile.MoveFirst();
				//cout << "here 1" << endl;
				tmpFile.GetNext(rightRec);
				//cout << "here 2" << endl;
				if (0 != cEng.Compare(lRec, &lOrder, &rightRec, &rOrder)) {
					tmpFile.Close();
					remove (tmpFileName);
					remove (metaFileName);
					dumpFileExists = false;
				} else {
					recBuffer->MergeRecords(lRec, &rightRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
					wArgs->out->Insert(recBuffer);
					delete recBuffer;
					recBuffer = new Record;
					while (tmpFile.GetNext(rightRec)) {
						recBuffer->MergeRecords(lRec, &rightRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
						wArgs->out->Insert(recBuffer);
						delete recBuffer;
						recBuffer = new Record;
					}
				}
			}
			delete lRec;
			lRec = new Record;

			if (skipCompare) {
				skipCompare = false;
			}

			readLeft = true;
			continue;
		}

		if (result > 0) {
			// right record is lagging behind
			delete rRec;
			rRec = new Record;
			readRight = true;
			continue;
		}

		if (result == 0) {
			// records match! merge and dump into a file for future use
			Record dumpThis;
			dumpThis.Copy(rRec);
			recBuffer->MergeRecords(lRec, rRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
			wArgs->out->Insert(recBuffer);

			if (!dumpFileExists) {
				//cout << "create..." << endl;
				if (FALSE == tmpFile.Create(tmpFileName, heap, NULL)) {
					cout << "Error creating dbfile!" << __FILE__ << __LINE__ << endl;
					exit(1);
				}
				dumpFileExists = true;
				// we do not want to merge the current record again
				skipCompare = true;
			}
			//cout << "add..." << endl;
			tmpFile.Add(dumpThis);
			//cout << "added!" << endl;

			delete recBuffer;
			recBuffer = new Record;
			readRight = true;
		}
	}

	if (dumpFileExists) {
		tmpFile.Close();
		remove (tmpFileName);
		remove (metaFileName);
		dumpFileExists = false;
	}

	delete lRec;
	delete rRec;
	delete recBuffer;

	wArgs->out->ShutDown();
}

void SortMergeJoin(workerArgs *wArgs) {
	// sort both relations
	// dump the right relation while there is a match
	// for it on the left relation
	OrderMaker lOrder, rOrder;

	int result = wArgs->cnf->GetSortOrders(lOrder, rOrder);

	// create 2 bigQs for sorting the l and the r relations
	Pipe lOutPipe(100), rOutPipe(100);
	BigQ lBigQ(*(wArgs->in), lOutPipe, lOrder, wArgs->runlen);
	BigQ rBigQ(*(wArgs->inR), rOutPipe, rOrder, wArgs->runlen);

	// compare the 1st record of both relations
	// until left < right, skip left
	// until left > right, skip right
	// until left == right, dump right into a file
	DBFile tmpFile;
	char tmpFileName[MAX_FILENAME_LEN];
	char metaFileName[MAX_FILENAME_LEN];
	sprintf(tmpFileName, "%u%u.dump\0", (unsigned int)getpid(), (unsigned int)pthread_self());
	sprintf(metaFileName, "%s.meta\0", tmpFileName);
	Record *lRec = new Record, *rRec = new Record, *recBuffer = new Record;
	ComparisonEngine cEng;

	bool readLeft = false, readRight = false, dumpFileExists = false, skipCompare = true;

	if (!lOutPipe.Remove(lRec)) {
		wArgs->out->ShutDown();
		return;
	}

	if (!rOutPipe.Remove(rRec)) {
		wArgs->out->ShutDown();
		return;
	}

	// get the configuration of left and right records
	int lNumAtts = GetRecAtts(*lRec);
	int rNumAtts = GetRecAtts(*rRec);
	int attToKeep[lNumAtts+rNumAtts];
	for (int i = 0; i < lNumAtts; i++) {
		attToKeep[i] = i;
	}
	for (int i = 0; i < rNumAtts; i++) {
		attToKeep[i+lNumAtts] = i;
	}

	while (true) {
		// if need to read from left relalation
		if (readLeft) {
			if (!lOutPipe.Remove(lRec)) {
				break;
			}
			readLeft = false;
		}

		// if need to read from right relation
		if (readRight) {
			if (!rOutPipe.Remove(rRec)) {
				break;
			}
			readRight = false;
		}

		result = cEng.Compare(lRec, &lOrder, rRec, &rOrder);
		if (result < 0) {
			// left record is lagging behind
			// first check if the dump file exists and compare
			// the first record with the lRec, if a match, all
			// records in the dump file should be merged with
			// lRec otherwise, close and delete the dump file
			Record rightRec;
			if (dumpFileExists && !skipCompare) {
				//cout << "here 0" << endl; 
				tmpFile.MoveFirst();
				//cout << "here 1" << endl;
				tmpFile.GetNext(rightRec);
				//cout << "here 2" << endl;
				if (0 != cEng.Compare(lRec, &lOrder, &rightRec, &rOrder)) {
					tmpFile.Close();
					remove (tmpFileName);
					remove (metaFileName);
					dumpFileExists = false;
				} else {
					recBuffer->MergeRecords(lRec, &rightRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
					wArgs->out->Insert(recBuffer);
					delete recBuffer;
					recBuffer = new Record;
					while (tmpFile.GetNext(rightRec)) {
						recBuffer->MergeRecords(lRec, &rightRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
						wArgs->out->Insert(recBuffer);
						delete recBuffer;
						recBuffer = new Record;
					}
				}
			}
			delete lRec;
			lRec = new Record;

			if (skipCompare) {
				skipCompare = false;
			}

			readLeft = true;
			continue;
		}

		if (result > 0) {
			// right record is lagging behind
			delete rRec;
			rRec = new Record;
			readRight = true;
			continue;
		}

		if (result == 0) {
			// records match! merge and dump into a file for future use
			Record dumpThis;
			dumpThis.Copy(rRec);
			recBuffer->MergeRecords(lRec, rRec, lNumAtts, rNumAtts, attToKeep, (lNumAtts+rNumAtts), lNumAtts);
			wArgs->out->Insert(recBuffer);

			if (!dumpFileExists) {
				//cout << "create..." << endl;
				if (FALSE == tmpFile.Create(tmpFileName, heap, NULL)) {
					cout << "Error creating dbfile!" << __FILE__ << __LINE__ << endl;
					exit(1);
				}
				dumpFileExists = true;
				// we do not want to merge the current record again
				skipCompare = true;
			}
			//cout << "add..." << endl;
			tmpFile.Add(dumpThis);
			//cout << "added!" << endl;

			delete recBuffer;
			recBuffer = new Record;
			readRight = true;
		}
	}

	if (dumpFileExists) {
		tmpFile.Close();
		remove (tmpFileName);
		remove (metaFileName);
		dumpFileExists = false;
	}

	delete lRec;
	delete rRec;
	delete recBuffer;

	wArgs->out->ShutDown();
}

void* workerJoin (void *args) {
        workerArgs *wArgs = (workerArgs *)args;

	OrderMaker lOrder, rOrder;

	int result = wArgs->cnf->GetSortOrders(lOrder, rOrder);
	if (0 == result) {
		NestedLoopJoin(wArgs);
		return NULL;
	} else {
		SortMergeJoin(wArgs);
		return NULL;
	}

	// create 2 bigQs for sorting the l and the r relations
	Pipe lOutPipe(100), rOutPipe(100);
	BigQ lBigQ(*(wArgs->in), lOutPipe, lOrder, wArgs->runlen);
	BigQ rBigQ(*(wArgs->inR), rOutPipe, rOrder, wArgs->runlen);

	Record *lRec = new Record, *rRec = new Record, *recBuffer = new Record;
	ComparisonEngine cEng;
	int attToKeep[] = {0, 1, 2, 3, 4 ,5, 6, 0, 1, 2, 3, 4};
	//numAttsL = ((int *) bits)[1]/(sizeof(int)) - 1;
	//nulAttsR = ((int *) bits)[1]/(sizeof(int)) - 1;
	bool nextLeft = true, nextRight = true;

	while (true) {

		//cout << cnt++ << " | " << outcnt << endl;

		if (nextLeft) {
			//cout << "Removing from pipe 1" << endl;
			//if (!wArgs->in->Remove(lRec)) {
			if (!lOutPipe.Remove(lRec)) {
				//cout << "LEFT" << endl;
				break;
			}
			//wArgs->in->Remove(&lRec);
			nextLeft = false;
		}

		if (nextRight) {
			//cout << "Removing from pipe 2" << endl;
			if (!rOutPipe.Remove(rRec)) {
				//cout << "RIGHT" << endl;
				break;
			}
			//rRec.Print(new Schema("catalog", "partsupp"));
			nextRight = false;
		}

		result = cEng.Compare(lRec, &lOrder, rRec, &rOrder);
		if (0 == result) {
			//lRec->Print(new Schema("catalog", "supplier"));
			//rRec->Print(new Schema("catalog", "partsupp"));
			recBuffer->MergeRecords(lRec, rRec, 7, 5, attToKeep, 12, 7);
			wArgs->out->Insert(recBuffer);

			delete recBuffer;
			recBuffer = new Record;

			nextRight = true;
		} else if (result < 0) {
			delete lRec;
			lRec = new Record;
			nextLeft = true;
		} else {
			delete rRec;
			rRec = new Record;
			nextRight = true;
		}
	}

	delete lRec;
	delete rRec;
	delete recBuffer;

        wArgs->out->ShutDown();
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {

        wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
        wArgs->in = &inPipeL;
	wArgs->inR = &inPipeR;
        wArgs->out = &outPipe;
	wArgs->cnf = &selOp;
	wArgs->literal = &literal;
	wArgs->runlen = runlen;

        // a successfull creation of thread returns 0
        if (pthread_create(&workerThread, NULL, workerJoin, (void *)wArgs)) {
                cout << "Failed to create join worker thread!" << endl;
        }
}

void Join :: WaitUntilDone() {

        pthread_join (workerThread, NULL);

        if (NULL != wArgs) {
                delete wArgs;
                wArgs = NULL;
        }
}

void Join :: Use_n_Pages (int runlen) {
        this->runlen = runlen;
}

//////////// DUPLICATE REMOVAL ////////////////
void* workerDuplicate (void *args) {
        workerArgs *wArgs = (workerArgs *)args;

	// sort the the table according to all attributes,
	// compare the adjoining attributes, if same skip
	// putting that into the out pipe

	// the input pipe given to duplicate removal
	// should be given to BigQ. the out pipe should be
	// filtered !!!

	Pipe intermOutPipe(100);
	OrderMaker sortOrder(wArgs->schema);

	// this will create a thread which perfroms sorting
	BigQ bigQ(*(wArgs->in), intermOutPipe, sortOrder, wArgs->runlen);

	// assigning them dynamically because if the record is
	// duplicate and not worth consuming, I can delete it
	Record *prev = new Record;
	Record *curr = new Record;

	// remove the 1st record, it is never a duplicate of anyone
	// insert that into the out pipe. if no records
	// we are done
	if (!intermOutPipe.Remove(prev)) {
		intermOutPipe.ShutDown();
		wArgs->out->ShutDown();
		delete prev;
		delete curr;
		return NULL;
	}

	ComparisonEngine cEng;

	// remove all records from the intermediate pipe
	// compare with prev, if same do not insert into
	// the out pipe
	while (intermOutPipe.Remove(curr)) {
		if (0 == cEng.Compare(prev, curr, &sortOrder)) {
			delete curr;
			curr = new Record;
			continue;
		}

		// inserting into pipe, consumes prev pointer
		// and hence we can overwrite it now
		wArgs->out->Insert(prev);
		prev->Copy(curr);
		delete curr;
		curr = new Record;
	}

	wArgs->out->Insert(prev);
	delete prev;
	delete curr;

	intermOutPipe.ShutDown();
        wArgs->out->ShutDown();
}

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

        wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
        wArgs->in = &inPipe;
        wArgs->out = &outPipe;
	wArgs->schema = &mySchema;
	wArgs->runlen = runlen;

        // a successfull creation of thread returns 0
        if (pthread_create(&workerThread, NULL, workerDuplicate, (void *)wArgs)) {
                cout << "Failed to create duplicate removal worker thread!" << endl;
        }
}

void DuplicateRemoval :: WaitUntilDone() {

        pthread_join (workerThread, NULL);

        if (NULL != wArgs) {
                delete wArgs;
                wArgs = NULL;
        }
}

void DuplicateRemoval :: Use_n_Pages (int runlen) {
        this->runlen = runlen;
}

//////////////////// GROUP BY //////////////////////////
void* workerGroupBy (void *args) {
        workerArgs *wArgs = (workerArgs *)args;

	// sort by the columns on which it is to be grouped by
	Pipe intermOutPipe(100);
	BigQ bigQ(*(wArgs->in), intermOutPipe, *(wArgs->order), wArgs->runlen);

	// assigning them dynamically because if the record is
        // duplicate and not worth consuming, I can delete it
        Record *prev = new Record;
        Record *curr = new Record;

        // remove the 1st record, it is never a duplicate of anyone
        // insert that into the out pipe. if no records
        // we are done
        if (!intermOutPipe.Remove(prev)) {
                intermOutPipe.ShutDown();
                wArgs->out->ShutDown();
                delete prev;
                delete curr;
                return NULL;
        }

	// the record that has been removed from the intermOutPipe
	// is always distinct and hence pass it to the sum's in pipe
	Pipe *sumInPipe = new Pipe(100);
	Record sumRecBuffer;
	
	Sum aggr;
	aggr.Run(*sumInPipe, *(wArgs->out), *(wArgs->func));
	sumRecBuffer.Copy(prev);

	sumInPipe->Insert(&sumRecBuffer);
        
	// remove all records from the intermediate pipe
        // compare with prev, if same do not insert into
        // the out pipe
	ComparisonEngine cEng;
        while (intermOutPipe.Remove(curr)) {
                if (0 == cEng.Compare(prev, curr, wArgs->order)) {
			// apply aggregate function if the records are similar
			sumInPipe->Insert(curr);
                        continue;
                }

		// if you have reached here it means that it is a different
		// group now and hence get the aggregate record from the out pipe
		sumInPipe->ShutDown();
		aggr.WaitUntilDone();

                // inserting into pipe, consumes prev pointer
                // and hence we can overwrite it now
		delete prev;
		prev = new Record;
                prev->Copy(curr);
		delete curr;
		curr = new Record;

		// start a new instance of sum now
		delete sumInPipe;
		sumInPipe = new Pipe(100);
		sumRecBuffer.Copy(prev);
		sumInPipe->Insert(&sumRecBuffer);
		aggr.Run(*sumInPipe, *(wArgs->out), *(wArgs->func));
        }

	// take care of the last record
	sumInPipe->ShutDown();
	aggr.WaitUntilDone();

	// release everything
        delete prev;
        delete curr;
	delete sumInPipe;

        wArgs->out->ShutDown();
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {

        wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
        wArgs->in = &inPipe;
        wArgs->out = &outPipe;
	wArgs->order = &groupAtts;
	wArgs->func = &computeMe;
	wArgs->runlen = runlen;

        // a successfull creation of thread returns 0
        if (pthread_create(&workerThread, NULL, workerGroupBy, (void *)wArgs)) {
                cout << "Failed to create group by worker thread!" << endl;
        }
}

void GroupBy :: WaitUntilDone() {

        pthread_join (workerThread, NULL);

        if (NULL != wArgs) {
                delete wArgs;
                wArgs = NULL;
        }
}

void GroupBy :: Use_n_Pages (int runlen) {
        this->runlen = runlen;
}

//////////////////// WRITEOUT ////////////////////////
void* workerWriteOut (void *args) {
        workerArgs *wArgs = (workerArgs *)args;

	// simulate the Record.Print() function,
	// instead of cout, write to file
	
	Record *recBuffer = new Record;

	while (wArgs->in->Remove(recBuffer)) {
		recBuffer->WriteToFile(wArgs->file, wArgs->schema);
		delete recBuffer;
		recBuffer = new Record;
	}

	delete recBuffer;
}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

        wArgs = new workerArgs;
	memset(wArgs, 0x0, sizeof(workerArgs));
        wArgs->in = &inPipe;
        wArgs->file = outFile;
	wArgs->schema = &mySchema;

        // a successfull creation of thread returns 0
        if (pthread_create(&workerThread, NULL, workerWriteOut, (void *)wArgs)) {
                cout << "Failed to create write out worker thread!" << endl;
        }
}

void WriteOut :: WaitUntilDone() {

        pthread_join (workerThread, NULL);

        if (NULL != wArgs) {
                delete wArgs;
                wArgs = NULL;
        }
}

void WriteOut :: Use_n_Pages (int runlen) {
        this->runlen = runlen;
}
