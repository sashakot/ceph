#include "UCXStack.h"
#include "UCXEvent.h"

#include "common/errno.h"

int UCXDriver::init(EventCenter *c, int nevent)
{
	return 0;
}

int UCXDriver::add_event(int fd, int cur_mask, int add_mask)
{
	return 0;
}

int UCXDriver::del_event(int fd, int cur_mask, int delmask)
{
	return 0;
}

int UCXDriver::resize_events(int newsize)
{
	return 0;
}

int UCXDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
    return 0;
}

















