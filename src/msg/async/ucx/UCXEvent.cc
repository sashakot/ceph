#include "UCXStack.h"
#include "UCXEvent.h"

#include "common/errno.h"

//#define dout_subsys ceph_subsys_ms

//#undef dout_prefix
//#define dout_prefix *_dout << "UCXDriver."

int UCXDriver::init(EventCenter *c, int nevent)
{
	return EpollDriver::init(c, nevent);
}

int UCXDriver::add_event(int fd, int cur_mask, int add_mask)
{
	return EpollDriver::add_event(fd, cur_mask, add_mask);
}

int UCXDriver::del_event(int fd, int cur_mask, int delmask)
{
	return EpollDriver::del_event(fd, cur_mask, delmask);
}

int UCXDriver::resize_events(int newsize)
{
	return EpollDriver::resize_events(newsize);
}

int UCXDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
    return EpollDriver::event_wait(fired_events, tvp);
}

















