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

    char data[0];
};

struct ucx_req_descr {
    bufferlist *bl;
    ucp_dt_iov_t *iov_list;
    ucx_rx_buf *rx_buf;
    void *rx_queue;
};

class DummyDataType {
    public:
        DummyDataType() {
            ucs_status_t status = ucp_dt_create_generic(
                                            &dummy_datatype_ops,
                                            NULL, &ucp_datatype);
            if (status != UCS_OK) {
                ceph_abort();
            }
        };

        ~DummyDataType() {
            ucp_dt_destroy(ucp_datatype);
        }

        static void* dummy_start_cb(void *context,
                                    void *buffer,
                                    size_t count) {
            return NULL;
        }

        static size_t dummy_pack_cb(void *state, size_t offset,
                                    void *dest, size_t max_length) {
            return max_length;
        }

        static ucs_status_t dummy_unpack_cb(void *state, size_t offset,
                                            const void *src, size_t count) {
            return UCS_OK;
        }

        static size_t dummy_datatype_packed_size(void *state) {
            return 0;
        }

        static void dummy_datatype_finish(void *state) {
        }

        static void dummy_completion_cb(void *req, ucs_status_t status,
                                 ucp_tag_recv_info_t *info) {
             ucp_request_free(req);
        }

        ucp_datatype_t ucp_datatype;
        static const ucp_generic_dt_ops_t dummy_datatype_ops;
};

struct connect {
   ucp_ep_h *ep;
   EventCallbackRef conn_cb;
};

class UCXDriver : public EpollDriver {
    private:
        CephContext *cct;

        int undelivered = 0;

        int ucp_fd = -1;
        ucp_worker_h ucp_worker;

        std::set<int> connections;
        std::set<int> waiting_events;

        DummyDataType dummy_dtype;

        std::map<int, struct connect> connecting;
        std::map<int, std::deque<ucx_rx_buf *>> queues;

        void event_progress(vector<FiredFileEvent> &fired_events);
        void dispatch_events(vector<FiredFileEvent> &fired_events);

        void insert_zero_msg(int fd);
        void recv_msg(int fd, ucp_tag_message_h msg,
                      ucp_tag_recv_info_t &msg_info);

        static void dispatch_rx(ucx_rx_buf *buf,
                                void *qptr);

        bool in_set(std::set<int> set, int fd) {
            return set.find(fd) != set.end();
        }

        char *recv_addr(int fd, uint64_t *dst_tag);
        int send_addr(int fd, uint64_t tag,
                      ucp_address_t *ucp_addr,
                      size_t ucp_addr_len);

        int conn_create(int fd);

    public:
        UCXDriver(CephContext *c): EpollDriver(c), cct(c), dummy_dtype() {}
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
                           ucp_ep_h *ep,
                           uint64_t tag,
                           EventCallbackRef conn_cb,
                           ucp_address_t *ucp_addr,
                           size_t ucp_addr_len);

        void drop_events(int fd);
        void conn_close(int fd, ucp_ep_h ucp_ep);

        ucx_rx_buf *get_rx_buf(int fd);
        void pop_rx_buf(int fd);

        static void recv_completion_cb(void *request,
                                       ucs_status_t status,
                                       ucp_tag_recv_info_t *info);
};


#endif //CEPH_UCXEVENT_H
