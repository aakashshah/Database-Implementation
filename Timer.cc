#include "Timer.h"
#include <iostream>

using namespace std;

Timer :: Timer() {
	startTime = (clock_t)0;
}

void Timer :: Start() {
	startTime = clock();
}

clock_t Timer :: Stop() {
	if (0 == startTime) {
		return 0;
	}

	return ((clock() - startTime)/CLOCKS_PER_MSEC);
}
