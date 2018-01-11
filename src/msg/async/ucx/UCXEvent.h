#ifndef CEPH_UCXEVENT_H
#define CEPH_UCXEVENT_H

#include <vector>

#include "msg/async/Stack.h"
#include "msg/async/Event.h"
#include "msg/async/EventEpoll.h"

extern "C" {
#include <ucp/api/ucp.h>
};

struct ucx_rx_buf {
    size_t length;
    size_t offset;

    uint8_t *rdata;
};

struct ucx_req_descr {
    bufferlist *bl;
    ucp_dt_iov_t *iov_list;
    ucx_rx_buf *rx_buf;
    void *rx_queue;
};

typedef struct {
    uint64_t  dst_tag;  //Vasily: remove it
    ucp_ep_h  ucp_ep;
    std::deque<bufferlist *> pending;
    std::deque<ucx_rx_buf *> rx_queue;
} connection_t;

class UCXDriver : public EpollDriver {
    private:
        CephContext *cct;

        int undelivered = 0;

        int ucp_fd = -1;
        ucp_worker_h ucp_worker;

        std::set<int> connecting;
        std::set<int> waiting_events;

        std::map<int, connection_t> connections;

        void event_progress(vector<FiredFileEvent> &fired_events);
        void dispatch_events(vector<FiredFileEvent> &fired_events);

        void insert_rx(int fd, uint8_t *rdata, size_t length);
        void recv_msg(int fd, ucp_tag_message_h msg,
                      ucp_tag_recv_info_t &msg_info);

        bool in_set(std::set<int> set, int fd) {
            return set.find(fd) != set.end();
        }

        char *recv_addr(int fd, uint64_t *dst_tag);
        int send_addr(int fd, uint64_t tag,
                      ucp_address_t *ucp_addr,
                      size_t ucp_addr_len);

        int conn_create(int fd);
        int recv_stream(int fd);

    public:
        UCXDriver(CephContext *c): EpollDriver(c), cct(c) {}
        virtual ~UCXDriver();

        int init(EventCenter *c, int nevent) override;
        int add_event(int fd, int cur_mask, int add_mask) override;
        int del_event(int fd, int cur_mask, int del_mask) override;
        int resize_events(int newsize) override;
        int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) override;

        void cleanup(ucp_address_t *ucp_addr);
        void addr_create(ucp_context_h ucp_context,
                         ucp_address_t **ucp_addr,
                         size_t *ucp_addr_len);

        int conn_establish(int fd,
                           ucp_address_t *ucp_addr,
                           size_t ucp_addr_len);

        void conn_close(int fd);

        int is_connected(int fd) {
            return (connections.count(fd) > 0 &&
                        NULL != connections[fd].ucp_ep);
        }

        ssize_t send(int fd, bufferlist &bl, bool more);
        static void send_completion_cb(void *request, ucs_status_t status);

        static void send_completion(ucx_req_descr *descr) {
            descr->bl->clear();
            if (descr->iov_list) {
                delete descr->iov_list;
            }
        }

        int read(int fd, char *rbuf, size_t bytes);
};


#endif //CEPH_UCXEVENT_H
