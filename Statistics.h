#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <string>

using namespace std;

class Statistics {
private:
	// maps the relation name => tuples
	map<string, int> relMap;

	// maps the attribute name => relation name
	map<string, string> attToRelMap;

	// maps relation name => no of distinct tuples
	typedef map<string, int> m;
	typedef map <string, m> aMap;
	aMap attMap;

	// map that remembers the current relations
	// which are joined
	map <string, string> joinMap;

	// iterators
	map<string, int> :: iterator relMapIt;
	map<string, string> :: iterator attToRelMapIt;
	aMap :: iterator attMapIt;
	m :: iterator attMapValueIt;
	map<string, string> :: iterator joinMapIt;

	void Print();
	string GetNewRelName(string A, string B);
	int GetTuplesFromAttribute(char *attName);
	int GetDistinctFromAttribute(char *attName);
	string GetRelNameFromAttName(char *attName);
	double EstimateAttr(struct ComparisonOp *operands, int &relTuples, int &join);
	void ApplyAttr(struct OrList *orList);
	bool InvalidInput();
	void UpdateRelAndAttr(char *name, int value);
	void ApplyOnOrGroup(struct ComparisonOp *operands, double estimate);

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
