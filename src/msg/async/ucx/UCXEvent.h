#ifndef CEPH_UCXEVENT_H
#define CEPH_UCXEVENT_H

#include "msg/async/Stack.h"
#include "msg/async/EventEpoll.h"

class UCXDriver : public EpollDriver {
    private:
        CephContext *cct;

    public:
        UCXDriver(CephContext *c): EpollDriver(c), cct(c) {}
        virtual ~UCXDriver() {}

        int init(EventCenter *c, int nevent) override;
        int add_event(int fd, int cur_mask, int add_mask) override;
        int del_event(int fd, int cur_mask, int del_mask) override;
        int resize_events(int newsize) override;
        int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) override;
};

#endif //CEPH_UCXEVENT_H
