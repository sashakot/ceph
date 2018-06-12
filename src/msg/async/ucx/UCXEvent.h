#ifndef CEPH_UCXEVENT_H
#define CEPH_UCXEVENT_H

#include <vector>

//Vasily: ???
//#include <cinttypes>
//#include <cstdint>
//#include <limits.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "msg/async/Stack.h"
#include "msg/async/Event.h"
#include "msg/async/EventEpoll.h"

#include "common/Mutex.h"

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

struct ucx_connect_message {
    uint16_t addr_len;
    uint32_t remote_fd;
} __attribute__ ((packed));

typedef struct {
    ucp_ep_h  ucp_ep;
    std::deque<ucx_rx_buf *> rx_queue;
} connection_t;

typedef struct {
    int fd;
    ucp_ep_address_t *ep_addr;
} accept_t;

class UCXDriver : public EpollDriver {
    private:
        CephContext *cct;

        int ucp_fd = -1;
        ucp_worker_h ucp_worker = NULL;

        Mutex lock; /* Protects 'accepted' pool */
        std::deque<accept_t *> accepted;

        std::set<int> read_events;
        std::set<int> write_events;

        std::set<int> undelivered;
        std::map<int, connection_t> connections;

        std::set<int> conn_requests;

        std::map<int, ucp_listener_h> listeners;
        std::map<int, std::deque<ucp_ep_address_t *>> accept_queues;

        void event_progress();

        void insert_rx(int fd, uint8_t *rdata, size_t length);
        void queue_accept(int server_fd, ucp_ep_address_t *ep_addr);

        bool in_set(std::set<int> set, int fd) {
            return set.find(fd) != set.end();
        }

        int recv_stream(int fd);
        void conn_create(int fd, ucp_ep_h ucp_ep);

        void conn_release_recvs(int fd);
        void ucx_ep_close(int fd, bool close_event);

        int process_writes(vector<FiredFileEvent> &fired_events);
        int process_connections(vector<FiredFileEvent> &fired_events);

        int server_conn_create(int fd, ucp_ep_address_t *ucp_ep_addr);

        class {
            private:
                int base_fd = -1;
                std::set<int> in_use;

            public:
                int open_fd() {
                    int fd = dup(base_fd);

                    assert(fd > 0);
                    in_use.insert(fd);

                    return fd;
                }

                void close_fd(int fd) {
                    assert(in_use.count(fd) > 0);
                    in_use.erase(fd);
                    close(fd);
                }

                bool is_open(int fd) {
                    return in_use.count(fd) > 0;
                }

                bool is_system_fd(int fd) {
                    return !in_use.count(fd);
                }

                void init(int base) {
                    assert(base > 0);
                    assert(base_fd < 0);
                    base_fd = base;
                }
        } fd_pool;

    public:
        UCXDriver(CephContext *c): EpollDriver(c), cct(c),
                                   lock("UCXDriver::lock") {}
        virtual ~UCXDriver();

        int init(EventCenter *c, int nevent) override;
        int add_event(int fd, int cur_mask, int add_mask) override;
        int del_event(int fd, int cur_mask, int del_mask) override;
        int resize_events(int newsize) override;
        int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) override;

        void conn_close(int fd);
        void conn_shutdown(int fd);

        void cleanup();
        void worker_init(ucp_context_h ucp_context);

        int is_connected(int fd) {
            return (connections.count(fd) > 0 &&
                        NULL != connections[fd].ucp_ep);
        }

        ssize_t send(int fd, bufferlist &bl, bool more);
        static void accept_cb(ucp_ep_address_t *ep_addr, void *arg);

        static void send_completion_cb(void *request, ucs_status_t status);
        static void send_completion(ucx_req_descr *descr) {
            descr->bl->clear();
            if (descr->iov_list) {
                delete descr->iov_list;
            }
        }

        ssize_t read(int fd, char *rbuf, size_t bytes);

        int listen(entity_addr_t &sa,
                   const SocketOptions &opt,
                   int &server_fd);

        int accept(int server_fd, int &fd,
                   ucp_ep_address_t *&ep_addr);

        int connect(const entity_addr_t& peer_addr,
                    const SocketOptions &opts, int &fd);

        void stop_listen(int server_fd);
        void abort_accept(int server_fd);

        int signal(int fd, ucp_ep_address_t *ep_addr);
};

#endif //CEPH_UCXEVENT_H
