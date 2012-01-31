#include <time.h>

#define MSEC_PER_SEC 1000
#define CLOCKS_PER_MSEC (CLOCKS_PER_SEC / MSEC_PER_SEC)

using namespace std;

class Timer {
private:
	clock_t startTime;
public:
	// initialize everything to zero
	Timer();

	// store the current time in ms into 'startTime'
	void Start();

	// calculate the time difference in ms
	clock_t Stop();
};
