#ifndef DEFS_H
#define DEFS_H

#define TRUE 1
#define FALSE 0

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();

#define MAX_FILENAME_LEN 128

#endif

