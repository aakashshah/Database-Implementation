#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

#define DEFAULT_RUN_LEN 50
#define DEFAULT_RECORD_SIZE 1024

typedef struct sortArgs_s {
	Pipe *in;
	Pipe *out;
	OrderMaker *sortOrder;
	int runlen;
}sortArgs;

typedef struct pQ_s {
	Record *rec;
	int runIndex;
}pQ;

using namespace std;

class BigQ {
	// this thread does the 2 phase sorting
	pthread_t sorterThread;
	sortArgs *args;

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
