#include "Statistics.h"
#include <iostream>
#include <stdio.h>
#include <string.h>

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
	// copy all maps
	relMap.insert(copyMe.relMap.begin(), copyMe.relMap.end());
	//attToRelMap.insert(copyMe.attToRelMap.begin(), copyMe.attToRelMap.end());
	joinMap.insert(copyMe.joinMap.begin(), copyMe.joinMap.end());

	attMapIt = copyMe.attMap.begin();
	while (attMapIt != copyMe.attMap.end()) {
		attMap.insert(make_pair(attMapIt->first, m()));
		attMap[attMapIt->first].insert(attMapIt->second.begin(), attMapIt->second.end());
	}
}

Statistics::~Statistics() {

	// clear everything
	relMap.clear();
	attToRelMap.clear();

	attMapIt = attMap.begin();
	while (attMapIt != attMap.end()) {
		attMapIt->second.clear();
		attMapIt++;
	}
	attMap.clear();

	joinMap.clear();
}

void Statistics::AddRel(char *relName, int numTuples) {
	// update into the rel map
	string rel;
	rel.append(relName);
	relMap.insert(pair<string, int>(relName, numTuples));
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
	// update into the att map
	string rel, att;
	rel.append(relName);
	att.append(attName);

	//cout << "Rel: " << rel << " Att: " << att << " Dist: " << numDistincts << endl;

	// if the relation is already existing, find and update
	// otherwise create a new object
	attMapIt = attMap.find(rel);
	if (attMapIt == attMap.end()) {
		//m value;
		//value.insert(make_pair(att, numDistincts));
		//attMap.insert(make_pair(rel, value));
		attMap.insert(make_pair(rel, m()));
	} /*else {
		//attMapIt->second.insert(make_pair(att, numDistincts));
	}*/
	attMap[rel].insert(make_pair(att, numDistincts));

	attToRelMap.insert(make_pair(att, rel));
}

void Statistics::CopyRel(char *oldName, char *newName) {
	// find the old name in relMap and attMap
	// make duplicates of both

	// populating the relMap
	relMapIt = relMap.find(oldName);
	// if value is not found
	if (relMapIt == relMap.end()) {
		cout << "Unable to find " << oldName << " in relMap" << endl;
		return;
	}

	AddRel(newName, relMapIt->second);

	attMapIt = attMap.find(oldName);
	if (attMapIt == attMap.end()) {
		cout << "Unable to find " << oldName << " in attMap" << endl;
		// TODO:delete what was inserted in relMap
		return;
	}

	// populating the attMap
	// while copying the attributes, append the relation name along with it
	char buffer[128];
	int mapSize = attMapIt->second.size();
	attMapValueIt = attMapIt->second.begin();
	for (int i = 0; i < mapSize; i++) {
		sprintf(buffer, "%s.%s\0", newName, attMapValueIt->first.c_str());
		AddAtt(newName, buffer, attMapValueIt->second);
		attMapValueIt++;
	}	
}

void Statistics::Read(char *fromWhere) {
	
	FILE* file;
	char rel[128] = {0};
	char att[128] = {0};
	int value;
	int ret, count;

	// open file
	file = fopen(fromWhere, "r");
	if (NULL == file) {
		file = fopen(fromWhere,"w");
		fclose(file);
		return;
	}

	// populate DS from the file
	// get number of relations
	ret = fscanf(file, "%d", &count);
	if (EOF == ret) {
		return;
	}
	while (count--) {
		ret = fscanf(file, "%s", rel);
		if (0 != strcmp(rel, "START")) {
			cout << fromWhere << " is invalid!" << endl;
			return;
		}
		// relation name
		ret = fscanf(file, "%s", rel);
		// no of tuples
		ret = fscanf(file, "%d", &value);
		AddRel(rel, value);
	
		// read all attribute names
		while (true) {
			ret = fscanf(file, "%s", att);
			if (0 == strcmp("END", att)) {
				break;
			}
			ret = fscanf(file, "%d", &value);
			AddAtt(rel, att, value);
		}
	}

	// joinMap
	ret = fscanf(file, "%d", &count);
	while (count--) {
		ret = fscanf(file, "%s %s", rel, att);
		joinMap[rel] = att;
	}

	// close file
	fclose(file);
}

void Statistics::Write(char *fromWhere) {
	
	FILE *file;
	// open file
	file = fopen(fromWhere, "w");
	if (!file) {
		cout << "Error opening file for writing!" << endl;
		return;
	}
	
	// dump DS into the file
	fprintf(file, "%d\n", (int)relMap.size());
	for (relMapIt = relMap.begin(); relMapIt != relMap.end(); relMapIt++) {
		fprintf(file, "START\n");
		fprintf(file, "%s\n", relMapIt->first.c_str());
		fprintf(file, "%d\n", relMapIt->second);
		attMapIt = attMap.find(relMapIt->first);
		for (attMapValueIt = attMapIt->second.begin(); attMapValueIt != attMapIt->second.end(); attMapValueIt++) {
			fprintf(file, "%s\n", attMapValueIt->first.c_str());
			fprintf(file, "%d\n", attMapValueIt->second);
		}
		fprintf(file, "END\n");
	}

	// time to jump the joinMap
	fprintf(file, "%d\n", (int)joinMap.size());
	for (joinMapIt = joinMap.begin(); joinMapIt != joinMap.end(); joinMapIt++) {
		fprintf(file, "%s %s\n", joinMapIt->first.c_str(), joinMapIt->second.c_str());
	}
	
	// close file
	fclose(file);
}

string Statistics::GetNewRelName(string A, string B) {
	string rel;
	if (strcmp(A.c_str(), B.c_str()) > 0) {
		rel.append(B);
		rel.append(".");
		rel.append(A);
	} else {
		rel.append(A);
		rel.append(".");
		rel.append(B);
	}
	return rel;
}

void Statistics::Print()
{

	cout << "ATTMAP" << endl;
	for ( attMapIt=attMap.begin() ; attMapIt != attMap.end(); attMapIt++ ) {
		cout << "\n\nNew element\n" << (*attMapIt).first << endl;
		for( attMapValueIt=(*attMapIt).second.begin(); attMapValueIt != (*attMapIt).second.end(); attMapValueIt++)
			cout << (*attMapValueIt).first << " => " << (*attMapValueIt).second << endl;
	}
	cout << "ATTTORELMAP" << endl;
	for ( attToRelMapIt=attToRelMap.begin() ; attToRelMapIt != attToRelMap.end(); attToRelMapIt++ ) {

		cout << attToRelMapIt->first << "--> " << attToRelMapIt->second << endl;


	}
}



string Statistics::GetRelNameFromAttName(char *attName) {
	string att, rel;
	att.append(attName);
	attToRelMapIt = attToRelMap.find(att);
	if (attToRelMapIt == attToRelMap.end()) {
		cout << "Could not map " << att << " to a relation! Map Size: " << attToRelMap.size() << endl;
		return rel;
	}

	// attToRelMapIt->second stores the relname
	// now check if this relation has join with
	// any other relation
	rel = attToRelMapIt->second;
	while (true) {
		joinMapIt = joinMap.find(rel);
		if (joinMapIt == joinMap.end()) {
			break;
		}
		string tmp = rel;
		rel.clear();
		if (strcmp(tmp.c_str(), joinMapIt->second.c_str()) > 0) {
			rel.append(joinMapIt->second.c_str());
			rel.append(".");
			rel.append(tmp);
		} else {
			rel.append(tmp);
			rel.append(".");
			rel.append(joinMapIt->second.c_str());
		}
	}

	return rel;
}

int Statistics::GetTuplesFromAttribute(char *attName) {
	string relName = GetRelNameFromAttName(attName);
	relMapIt = relMap.find(relName);
	if (relMapIt == relMap.end()) {
		cout << "Error in GetTuplesFromAttribute" << endl;
		return 0;
	}

	return relMapIt->second;
}

int Statistics::GetDistinctFromAttribute(char *attName) {
	string relName = GetRelNameFromAttName(attName);
	string att;
	att.append(attName);

        attMapIt = attMap.find(relName);
	if (attMapIt == attMap.end()) {
                cout << "Error in GetDistinctFromAttribute" << endl;
                return 0;
        }

	attMapValueIt = attMapIt->second.find(att);
	if (attMapValueIt == attMapIt->second.end()) {
		cout << "Error in GetDintinct!" << endl;
		return 0;
	}

	return attMapValueIt->second;
}


double Statistics::EstimateAttr(struct ComparisonOp *operands, int &relTuples, int &join) {

	double estimate = 0;
	bool inequality = false;
	
	// estimation reduces by a factor of 3
	// if inequality exists
	if (operands->code != EQUALS) {
		inequality = true;
	}

	//JOIN
	if (operands->left->code == NAME && operands->right->code == NAME) {
		join = 1;
		double tuplesL = GetTuplesFromAttribute(operands->left->value);
		double tuplesR = GetTuplesFromAttribute(operands->right->value);
		double distL = GetDistinctFromAttribute(operands->left->value);
		double distR = GetDistinctFromAttribute(operands->right->value);

		double tmp = ((tuplesL * tuplesR) / max(distL, distR));
		relTuples = (int)tmp;
		return 0;
	}

	// SELECTION
	struct Operand *name;
	if (operands->left->code == NAME && operands->right->code != NAME) {
		name = operands->left;
	} else {// if (operands->left->code != NAME && operands->right->code == NAME) {
		name = operands->right;
	}

	// from the name of the operand, find the number of tuples
	// in the relation to which this attribute belongs to
	int noOfTuples = GetTuplesFromAttribute(name->value);
	int noOfDistinct = GetDistinctFromAttribute(name->value);

	if (!noOfDistinct) {
		cout << "GetDistinctFromAttribute returned 0!" << endl;
		return estimate;
	}

	// saved by divide by 0 error
	if (!inequality) {
		estimate = noOfTuples / noOfDistinct;
	} else {
		estimate = noOfTuples / 3;
	}

	estimate = ((noOfTuples - estimate) / noOfTuples);
	relTuples = noOfTuples;

	//cout << "Estimate for " << name->value << " is: " << estimate << endl;

	return estimate;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
	// parse the AND list
	// check if it is for selection or join
	// if for selection, find the minimum of all AND groups
	struct AndList *ait = parseTree;
	struct OrList *oit = NULL;
	double estimate = -1;
	int joinTuples = 0;
	int join = 0;
	int relTuples;
	while (ait) {
			map<string, double> orMap;
			map<string, int> orCountMap;
			map<string, int> :: iterator orMapIt;
			string orMaptmp;
			int dependent = 0;
		// each AND group is made up to several
		// ORs, find estimate of each attribute
		// and the maximum of them is the estimate
		// if that AND group
		oit = ait->left;
		double orEstimate = 1;
		// EstimateAttr returns the rejection probability
		// of all attributes, multiply them all and then
		// finally convert them into selection probability
		while (oit) {
				orEstimate *= EstimateAttr(oit->left, relTuples, join);
				orCountMap[oit->left->left->value] += 1; 
				if (orMap[oit->left->left->value] == 0)
					orMap[oit->left->left->value] = orEstimate;
				if (orCountMap[oit->left->left->value] >= 2)
				{
					orMaptmp = oit->left->left->value;
					dependent = 1;
				}
				oit = oit->rightOr;
			}
			//if (oit->left) { cout << "oit->left: " << oit->left << endl;}
			if (dependent)
			{
				double tmp = 1-orMap[orMaptmp];
				tmp *= orCountMap[orMaptmp];
				orEstimate = tmp;
			}

			else
			{
				orEstimate = (1 - orEstimate);
			}
			if (join == 1) {
				joinTuples = relTuples; 
				join = 0;
			}
		if (-1 != estimate) {
			estimate *= orEstimate;
		} else {
			estimate = orEstimate;
		}
			orMap.clear();
			orCountMap.clear();
			dependent = 0;	
			ait = ait->rightAnd;
		}
		if (joinTuples != 0)
			estimate = joinTuples*estimate;

		else
	estimate = relTuples*estimate;

	return estimate;
}

bool Statistics::InvalidInput() {
	return false;
}

void Statistics::UpdateRelAndAttr(char *name, int estimate) {
	// find the relation to which this attribute belongs
	string rel = GetRelNameFromAttName(name);

	//update now
	attMapIt = attMap.find(rel);
	if (attMapIt == attMap.end()) {
		cout << "Should have got the attribute name here in UpdateAttr" << endl;
		return;
	}

	attMapValueIt = attMapIt->second.begin();
	int mapItems = attMapIt->second.size();
	for (int i = 0; i < mapItems; i++) {
		// all attributes will retain their distinct value or
		// the new estimate, whichever is minimum
		attMapValueIt->second = min(attMapValueIt->second, estimate);
		attMapValueIt++;
	}

	// this attribute would have caused the relation to shrink
	relMapIt = relMap.find(rel);
	if (relMapIt == relMap.end()) {
		cout << "Impossible in UpdateAttr" << endl;
		return;
	}

	relMapIt->second = estimate;
}



void Statistics::ApplyAttr(struct OrList *list) {
	
	bool inequality = false;
	int relTuples;
	struct ComparisonOp *operands = list->left;
	

	//JOIN -- it has only 1 operand in the or list
	if (operands->left->code == NAME && operands->right->code == NAME) {
		double tuplesL = GetTuplesFromAttribute(operands->left->value);
		double tuplesR = GetTuplesFromAttribute(operands->right->value);
		double distL = GetDistinctFromAttribute(operands->left->value);
		double distR = GetDistinctFromAttribute(operands->right->value);
		
		//cout << "tuplesR" << tuplesR << "tuplesL" << tuplesL << "distL" << distL << "distR" << distR << endl;
		
		double tmp  = ((tuplesL * tuplesR) / max(distL, distR));
		relTuples = (int)tmp;
		// update all maps
		string leftRel = GetRelNameFromAttName(operands->left->value);
		string rightRel = GetRelNameFromAttName(operands->right->value);
		string newRelName = GetNewRelName(leftRel, rightRel);
		
		// 1. relMap
		relMap.erase(leftRel);
		relMap.erase(rightRel);
		relMap.insert(make_pair(newRelName, relTuples));

		// 2. attMap
		//left
		//cout << leftRel << endl;
		attMapIt = attMap.find(leftRel);
		if (attMapIt == attMap.end()) {
			cout << "Error in finding " << rightRel << " " << __LINE__ << " " << __FILE__ << endl;
			return;
		}

		attMapValueIt = attMapIt->second.begin();
		int mapSize = attMapIt->second.size();

		for (int i = 0; i < mapSize; i++) {
			// 3. attToRelMap
			attToRelMap.erase(attMapValueIt->first);
			AddAtt((char *)newRelName.c_str(), (char *)attMapValueIt->first.c_str(), attMapValueIt->second);
			attMapValueIt++;
		}
		attMapIt = attMap.find(leftRel);
		attMapIt->second.clear();
		attMap.erase(leftRel);

		//right
		//cout << rightRel << endl;
		attMapIt = attMap.find(rightRel);
		if (attMapIt == attMap.end()) {
			cout << "Error in finding " << rightRel << " " << __LINE__ << " " << __FILE__ << endl;
			return;
		}

		attMapValueIt = attMapIt->second.begin();
		mapSize = attMapIt->second.size();
		for (int i = 0; i < mapSize; i++) {
			// 3. attToRelMap
			attToRelMap.erase(attMapValueIt->first);
			AddAtt((char *)newRelName.c_str(), (char *)attMapValueIt->first.c_str(), attMapValueIt->second);
			attMapValueIt++;
		}
		attMapIt = attMap.find(rightRel);
		attMapIt->second.clear();
		attMap.erase(rightRel);

		// 4. joinMap
		joinMap[leftRel] = rightRel;
		joinMap[rightRel] = leftRel;

		return;
	}

	double finalEstimate = 1;
	double estimate;
	struct Operand *name;

	// SELECTION - selection could have many operands
	while (list) {
		operands = list->left;

		// estimation reduces by a factor of 3
		// if inequality exists
		inequality = false;
		if (operands->code != EQUALS) {
			inequality = true;
		}

		if (operands->left->code == NAME && operands->right->code != NAME) {
			name = operands->left;
		} else {// if (operands->left->code != NAME && operands->right->code == NAME) {
			name = operands->right;
		}

		// from the name of the operand, find the number of tuples
		// in the relation to which this attribute belongs to
		int noOfTuples = GetTuplesFromAttribute(name->value);
		int noOfDistinct = GetDistinctFromAttribute(name->value);

		if (!noOfDistinct) {
			cout << "GetDistinctFromAttribute returned 0!" << endl;
			return;
		}

		// saved by divide by 0 error
		if (!inequality) {
			estimate = noOfTuples / noOfDistinct;
		} else {
			estimate = noOfTuples / 3;
		}

		estimate = ((noOfTuples - estimate) / noOfTuples);
		relTuples = noOfTuples;
		finalEstimate *= estimate;

		list = list->rightOr;
	}

	finalEstimate = (1 - finalEstimate);
	finalEstimate *= relTuples;
	UpdateRelAndAttr(name->value, finalEstimate);
}

void Statistics::Apply(struct AndList *parseTree, char **relNames, int numToJoin) {
	// parse the AND list
	// check if it is for selection or join
	// if for selection, find the minimum of all AND groups
	struct AndList *ait = parseTree;
	struct OrList *oit = NULL;
	double estimate = -1;
	int relTuples;
	while (ait) {
		ApplyAttr(ait->left);
		ait = ait->rightAnd;
	}
}

void Statistics::ApplyOnOrGroup(struct ComparisonOp *operands, double estimate) {

	//JOIN
	if (operands->left->code == NAME && operands->right->code == NAME) {
		// update all maps
		string leftRel = GetRelNameFromAttName(operands->left->value);
		string rightRel = GetRelNameFromAttName(operands->right->value);
		string newRelName = GetNewRelName(leftRel, rightRel);
		
		// 1. relMap
		relMap.erase(leftRel);
		relMap.erase(rightRel);
		relMap.insert(make_pair(newRelName, estimate));

		// 2. attMap
		//left
		attMapIt = attMap.find(leftRel);
		if (attMapIt == attMap.end()) {
			cout << "Error" << endl;
			return;
		}

		attMapValueIt = attMapIt->second.begin();
		int mapSize = attMapIt->second.size();

		/*
		cout << attMapIt->first << endl;
		cout << attMapValueIt->first << endl;
		cout << attMapValueIt->second << endl;
		cout << mapSize << endl;
		*/

		for (int i = 0; i < mapSize; i++) {
			// 3. attToRelMap
			attToRelMap.erase(attMapValueIt->first);
			AddAtt((char *)newRelName.c_str(), (char *)attMapValueIt->first.c_str(), min((int)estimate, attMapValueIt->second));
			attMapValueIt++;
		}
		//attMap.erase(attMapIt->first);

		//right
		attMapIt = attMap.find(rightRel);
		if (attMapIt == attMap.end()) {
			cout << "Error" << endl;
			return;
		}

		attMapValueIt = attMapIt->second.begin();
		mapSize = attMapIt->second.size();
		for (int i = 0; i < mapSize; i++) {
			// 3. attToRelMap
			attToRelMap.erase(attMapValueIt->first);
			AddAtt((char *)newRelName.c_str(), (char *)attMapValueIt->first.c_str(), min((int)estimate, attMapValueIt->second));
			attMapValueIt++;
		}
		//attMap.erase(attMapIt->first);

		// 4. joinMap
		joinMap[leftRel] = rightRel;
		joinMap[rightRel] = leftRel;

		return;
	}

	// SELECTION
	struct Operand *name;
	if (operands->left->code == NAME && operands->right->code != NAME) {
		name = operands->left;
	} else {// if (operands->left->code != NAME && operands->right->code == NAME) {
		name = operands->right;
	}

	UpdateRelAndAttr(name->value, estimate);
}

