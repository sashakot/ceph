#include "UCXStack.h"
#include "UCXEvent.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "UCXDriver "

typedef struct {
    int fd;
    UCXDriver *driver;
} ucp_accept_arg_t;

void UCXDriver::ucx_ep_close(int fd, bool close_event)
{
    if (is_connected(fd)) {
        ucp_ep_h ucp_ep = connections[fd].ucp_ep;

        assert(NULL != ucp_ep);
        ldout(cct, 20) << __func__ << " fd: " << fd
                       << " ep=" << (void *)ucp_ep << dendl;

        conn_release_recvs(fd);

        ucs_status_ptr_t request =
                ucp_ep_close_nb(ucp_ep, UCP_EP_CLOSE_MODE_SYNC);
        if (NULL == request) {
            ldout(cct, 20) << __func__ << " ucp ep fd: "
                           << fd << " closed in place..." << dendl;
        } else if (UCS_PTR_IS_ERR(request)) {
            lderr(cct) << __func__ << " fd: " << fd
                       << " ucp_ep_close_nb call failed: err "
                       << ucs_status_string(UCS_PTR_STATUS(request)) << dendl;
            ceph_abort(); //Vasily: ????
        } else {
            /*
             * We don't care even the request is still in
             * UCS_INPROGRESS state - it's UCX responsility now
             */
            ucp_request_free(request);
        }

        connections[fd].ucp_ep = NULL;

        if (close_event) {
            insert_rx(fd, NULL, 0);
        }
    }

    ldout(cct, 20) << __func__ << " fd: " << fd << " exit..." << dendl;
}

void UCXDriver::conn_create(int fd, ucp_ep_h ucp_ep)
{
    connection_t &conn = connections[fd];

    assert(!is_connected(fd));
    ldout(cct, 20) << __func__ << " conn for fd: " << fd
                               << " is creating " << dendl;

    conn.ucp_ep = ucp_ep;

    assert(!conn.rx_queue.size());
    assert(is_connected(fd));

    ldout(cct, 20) << __func__ << " fd: " << fd
                   << " ep=" << (void *)conn.ucp_ep
                   << " connection was successfully created " << dendl;
}

void UCXDriver::conn_release_recvs(int fd)
{
    //Vasily: while (ucp_worker_progress(ucp_worker));
    std::deque<ucx_rx_buf *> &rx_queue = connections[fd].rx_queue;

    /* Free all undelivered receives */
    while (!rx_queue.empty()) {
        ucx_rx_buf *rx_buf = rx_queue.front();

        if (rx_buf->length > 0) {
            ucp_stream_data_release(connections[fd].ucp_ep, rx_buf->rdata);
        }

        rx_queue.pop_front();
        delete rx_buf;
    }

    undelivered.erase(fd);
}

int UCXDriver::connect(const entity_addr_t &peer_addr,
                       const SocketOptions &opts, int &fd)
{
    ucp_ep_params_t ep_params;

    fd = fd_pool.open_fd();
    ep_params.field_mask = UCP_EP_PARAM_FIELD_FLAGS     |
                           UCP_EP_PARAM_FIELD_SOCK_ADDR |
                           UCP_EP_PARAM_FIELD_USER_DATA;

    ep_params.user_data = reinterpret_cast<void *>(fd);
    ep_params.flags     = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
#if 1
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));

    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr =  inet_addr("12.210.8.58");
    addr.sin_port        = peer_addr.get_port();

    ep_params.sockaddr.addr    = (struct sockaddr *) &addr;
    ep_params.sockaddr.addrlen = sizeof(struct sockaddr);
#endif
#if 0
    ep_params.sockaddr.addr    = peer_addr.get_sockaddr();
    ep_params.sockaddr.addrlen = sizeof(struct sockaddr);
#endif

    ucp_ep_h ucp_ep;
    ucs_status_t status = ucp_ep_create(ucp_worker, &ep_params, &ucp_ep);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " creating UCP ep to addr failed wth status: "
                               << ucs_status_string(status) << dendl;
        return -EINVAL; //Vasily: ?????
    }

    ldout(cct, 0) << __func__ << " fd: " << fd
                  << " UCP ep: " << (void *)ucp_ep << " to addr: "
                  << peer_addr.get_sockaddr()
                  << " successfully created" << dendl;

    conn_create(fd, ucp_ep);
    //Vasily: event_progress(); ????

    return 0;
}

void UCXDriver::queue_accept(int server_fd, ucp_ep_address_t *ep_addr)
{
    conn_requests.insert(server_fd);
    accept_queues[server_fd].push_back(ep_addr);

    ldout(cct, 0) << __func__ << " server_fd: " << server_fd
                  << " UCP ep_addr: " << (void *)ep_addr << dendl;
}

void UCXDriver::accept_cb(ucp_ep_address_t *ep_addr, void *arg) { /* ep_addr is worker_addr + ep_addr of client */
    ucp_accept_arg_t *ctx = reinterpret_cast<ucp_accept_arg_t *>(arg);

    int server_fd = ctx->fd;
    UCXDriver *driver = ctx->driver;

    driver->queue_accept(server_fd, ep_addr);
    delete ctx;

    //Vasily: if I am here => it happens during progress !!!!, if I get CQ event before that 'progress' call
    //=> check two cases of ucp_worker_progress call: => I should check server/fd events right after wait_event enterance: maybe make sense to do progress before quit and check recvs&server only on entarce
    //Vasily: insert to the 'accepted' (using lock) <server_setup_socket, ep_addr>

    /*
        event_wait {
            1) check recvs/accepts => return if get smthg
            2) wait on CQ events
            3) progress
        }

        ** when I call for ucp_worker_progress in case of ep close => call it while (ucp_worker_progress(ucp_worker)); after the request is called
    */
}

void UCXDriver::abort_accept(int server_fd)
{
    if (fd_pool.is_open(server_fd)) {
        conn_requests.erase(server_fd);
        accept_queues[server_fd].clear();

        fd_pool.close_fd(server_fd);
    }
}

void UCXDriver::stop_listen(int server_fd)
{
    if (listeners[server_fd]) {
        abort_accept(server_fd);
        ucp_listener_destroy(listeners[server_fd]);
        listeners[server_fd] = NULL;
        listeners.erase(server_fd);
    }
}

int UCXDriver::listen(entity_addr_t &sa,
                      const SocketOptions &opts, //Vasily: ?????
                      int &server_fd)
{
    ucp_listener_params_t params;

    server_fd = fd_pool.open_fd();

    params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                        UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER2;
#if 1
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("12.210.8.58");

    addr.sin_port = sa.get_port();

    params.sockaddr.addr    = reinterpret_cast<const struct sockaddr*>(&addr);
    params.sockaddr.addrlen = sizeof(struct sockaddr);
#endif
#if 0
    params.sockaddr.addr    = sa.get_sockaddr();
    params.sockaddr.addrlen = sizeof(struct sockaddr);
#endif
    ucp_accept_arg_t *arg = new ucp_accept_arg_t();

    arg->fd = server_fd;
    arg->driver = this;

    params.accept_handler2.cb  = accept_cb;
    params.accept_handler2.arg = reinterpret_cast<void *>(arg);

    ucp_listener_h ucp_listener;

    /* Create a listener on the server side to listen on the given address.*/
    ucs_status_t status = ucp_listener_create(ucp_worker, &params,
                                              &ucp_listener);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " Failed to create UCP listener with status: "
                   << ucs_status_string(status) << dendl;
        return -EINVAL;
    }

    ldout(cct, 0) << __func__ << " Created listner to addr: "
                   << sa.get_sockaddr() << " server_fd: " << server_fd << dendl;

    listeners[server_fd] = ucp_listener;

    return 0;
}

int UCXDriver::server_conn_create(int fd, ucp_ep_address_t *ep_addr)
{
    ucp_ep_h ucp_ep;
    ucp_ep_params_t ep_params;

    assert(fd > 0);

    ldout(cct, 0) << __func__ << " fd: " << fd
                  << " ep addr: " << ep_addr << dendl;

    ep_params.ep_addr   = ep_addr;
    ep_params.user_data = reinterpret_cast<void *>(fd);

    ep_params.field_mask = UCP_EP_PARAM_FIELD_EP_ADDR |
                           UCP_EP_PARAM_FIELD_USER_DATA;

    ucs_status_t status = ucp_ep_create(ucp_worker,
                                        &ep_params, &ucp_ep);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to create UCP endpoint " << dendl;
        return -EINVAL;
    }

    conn_create(fd, ucp_ep); //Vasily: ????
    return 0;
}

int UCXDriver::signal(int fd, ucp_ep_address_t *ep_addr)
{
    accept_t *c = new accept_t();

    c->fd = fd;
    c->ep_addr = ep_addr;

    Mutex::Locker l(lock);
    accepted.push_back(c);

    ucs_status_t status = ucp_worker_signal(ucp_worker);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to signal to ucp_worker: "
                               << (void *)ucp_worker << dendl;
        return -EINVAL; //Vasily: ????
    }

    return 0;
}

int UCXDriver::accept(int server_fd, int &fd,
                      ucp_ep_address_t *&ep_addr)
{
    if (accept_queues[server_fd].empty()) {
        return -EAGAIN;
    }

    ep_addr = accept_queues[server_fd].front();
    accept_queues[server_fd].pop_front();

    if (accept_queues[server_fd].empty()) {
        conn_requests.erase(server_fd);
    }

    fd = fd_pool.open_fd();
    ldout(cct, 0) << __func__ << " server_fd: "
                  << server_fd << " conn fd: " << fd << dendl;

    return 0;
}

void UCXDriver::conn_shutdown(int fd)
{
    ldout(cct, 20) << __func__ << " conn fd: " << fd << dendl;

    if (is_connected(fd)) {
        ucx_ep_close(fd, false);
    }

    read_events.erase(fd);
    write_events.erase(fd);
#if 0
    Mutex::Locker l(lock);
    if (accepted.count(fd) > 0) {
        accepted.erase(fd);
        assert(waiting_events.count(fd) == 0);
    }
#endif //Vasily: ???
}

void UCXDriver::conn_close(int fd)
{
    if (connections.count(fd) > 0) {
        assert(!read_events.count(fd) &&
                    !write_events.count(fd));
        /* Zero length messages cleaning */
        conn_release_recvs(fd);
        connections.erase(fd);
        fd_pool.close_fd(fd);
    }
}

void UCXDriver::worker_init(ucp_context_h ucp_context)
{
    ucs_status_t status;
    ucp_worker_params_t params;
#if 0
    if (id == 0)
        ucp_worker_print_info(ucp_worker, stdout);
#endif
    // TODO: check if we need a multi threaded mode
    params.thread_mode = UCS_THREAD_MODE_SINGLE;
    params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;

    assert(NULL != ucp_context);

    status = ucp_worker_create(ucp_context, &params, &ucp_worker);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to create UCP worker " << dendl;
        ceph_abort();
    }

    status = ucp_worker_get_efd(ucp_worker, &ucp_fd);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to obtain UCP worker event fd " << dendl;
        ceph_abort();
    }

    fd_pool.init(ucp_fd);
    EpollDriver::add_event(ucp_fd, 0, EVENT_READABLE);
}

void UCXDriver::cleanup()
{
    if (ucp_fd > 0) {
        ldout(cct, 20) << __func__ << dendl;

        assert(NULL != ucp_worker);

        EpollDriver::del_event(ucp_fd, EVENT_READABLE, EVENT_READABLE);
        ucp_worker_destroy(ucp_worker);

        ucp_fd     = -1;
        ucp_worker = NULL;
    }
}

UCXDriver::~UCXDriver()
{
}

ssize_t UCXDriver::send(int fd, bufferlist &bl, bool more)
{
    ucx_req_descr *req;
    ucp_dt_iov_t *iov_list;

    unsigned total_len = bl.length();
    unsigned iov_cnt = bl.get_num_buffers();

    if (total_len == 0) {
        return 0;
    }

    if (NULL == connections[fd].ucp_ep) {
        ldout(cct, 20) << __func__ << " fd: " << fd << " put "
                       << total_len << " bytes to the pending" << dendl;

        return total_len;
    }

    ldout(cct, 20) << __func__ << " fd: " << fd << " sending "
                               << total_len << " bytes. iov_cnt "
                               << iov_cnt << dendl;

    std::list<bufferptr>::const_iterator i = bl.buffers().begin();

    if (iov_cnt == 1) {
        iov_list = NULL;
        req = static_cast<ucx_req_descr *>(
                        ucp_stream_send_nb(
                            connections[fd].ucp_ep,
                            i->c_str(), i->length(),
                            ucp_dt_make_contig(1),
                            send_completion_cb, 0));
    } else {
        iov_list = new ucp_dt_iov_t[iov_cnt];

        for (int n = 0; i != bl.buffers().end(); ++i, n++) {
            iov_list[n].buffer = (void *)(i->c_str());
            iov_list[n].length = i->length();
        }

        req = static_cast<ucx_req_descr *>(
                        ucp_stream_send_nb(
                            connections[fd].ucp_ep,
                            iov_list, iov_cnt,
                            ucp_dt_make_iov(),
                            send_completion_cb, 0));
    }

    if (req == NULL) {
        /* in place completion */
        ldout(cct, 20) << __func__ << " SENT IN PLACE " << dendl;

        if (iov_list) {
           delete iov_list;
        }

        bl.clear();
        return 0;
    }

    if (UCS_PTR_IS_ERR(req)) {
        lderr(cct) << __func__ << " fd: " << fd << " send failure: " << UCS_PTR_STATUS(req) << dendl;
        return -EINVAL;
    }

    req->bl->claim_append(bl);
    req->iov_list = iov_list;

    ldout(cct, 0) << __func__ << " send in progress req " << req << dendl;

    return total_len;
}

void UCXDriver::send_completion_cb(void *req, ucs_status_t status)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    UCXDriver::send_completion(desc);
    ucp_request_free(req);

    std::cout << __func__ << " completed send req: " << (void *) req << std::endl; fflush(stderr); fflush(stdout);
}

void UCXDriver::insert_rx(int fd, uint8_t *rdata, size_t length)
{
    ucx_rx_buf *rx_buf = new ucx_rx_buf();

    rx_buf->offset = 0;
    rx_buf->rdata  = rdata;
    rx_buf->length = length;

    if (in_set(read_events, fd)) {
        undelivered.insert(fd);
    }

    connections[fd].rx_queue.push_back(rx_buf);

    ldout(cct, 20) << __func__ << " fd: " << fd << " insert rx buff "
                   << (void *)rx_buf << " of length=" << length << dendl;
}

int UCXDriver::recv_stream(int fd)
{
    size_t length;
    ucp_ep_h ucp_ep = connections[fd].ucp_ep;

    assert(ucp_ep);

    while (true) {
        uint8_t *rdata =
            reinterpret_cast<uint8_t *>(ucp_stream_recv_data_nb(ucp_ep, &length));
        if (UCS_PTR_IS_ERR(rdata)) {
            lderr(cct) << __func__ << " failed to receive data from UCP stream " << dendl;
            return -EINVAL;
        }

        /* receive nothing */
        if (UCS_PTR_STATUS(rdata) == UCS_OK) {
            break;
        }

        insert_rx(fd, rdata, length);
    }

    return 0;
}

ssize_t UCXDriver::read(int fd, char *rbuf, size_t bytes)
{
    assert(connections.count(fd) > 0);

    std::deque<ucx_rx_buf *> &rx_queue = connections[fd].rx_queue;
    if (rx_queue.empty()) {
        return -EAGAIN;
    }

    size_t left = 0;

    ucx_rx_buf *rx_buf = rx_queue.front();
    if (!rx_buf->length) {
        ldout(cct, 20) << __func__ << " fd: " << fd << " Zero length packet..." << dendl;
        goto erase_buf;
    }

    left = rx_buf->length - rx_buf->offset;
    ldout(cct, 20) << __func__ << " fd: " << fd << " read to "
                               << (void *)rbuf << " wanted "
                               << bytes << " left " << left << " rx_buf= "
                               << (void *)rx_buf << " rx_buf->length= "
                               << rx_buf->length << dendl;

    if (bytes < left) {
        memcpy(rbuf, rx_buf->rdata + rx_buf->offset, bytes);
        rx_buf->offset += bytes;

        return bytes;
    }

    memcpy(rbuf, rx_buf->rdata + rx_buf->offset, left);
    ucp_stream_data_release(connections[fd].ucp_ep, rx_buf->rdata);

erase_buf:
    rx_queue.pop_front();
    delete rx_buf;

    if (connections[fd].rx_queue.empty()) {
        undelivered.erase(fd);
    }

    return left;
}

void UCXDriver::event_progress()
{
    if (connections.empty()) {
        ldout(cct, 20) << __func__ << " The connections list is empty " << dendl;
        //return;
    }

    ssize_t count;

    do {
        const size_t max_eps = 10; //Vasily: ?????
        ucp_stream_poll_ep_t poll_eps[max_eps];

        /*
         * Look at 'ucp_worker_arm' usage example (ucp.h).
         * All existing events must be drained before waiting
         * on the file descriptor, this can be achieved by calling
         * 'ucp_worker_progress' repeatedly until it returns 0.
         */
        while (ucp_worker_progress(ucp_worker));

        count = ucp_stream_worker_poll(ucp_worker, poll_eps, max_eps, 0);
        for (ssize_t i = 0; i < count; ++i) {
            int fd = static_cast<int>(uintptr_t(poll_eps[i].user_data));

            assert(fd > 0);
            assert(connections[fd].ucp_ep == poll_eps[i].ep);
            assert((UCP_STREAM_POLL_FLAG_NVAL |
                        UCP_STREAM_POLL_FLAG_IN) & poll_eps[i].flags);

            if (UCP_STREAM_POLL_FLAG_IN & poll_eps[i].flags) {
                recv_stream(fd);
            }

            if (UCP_STREAM_POLL_FLAG_NVAL & poll_eps[i].flags) {
                ucx_ep_close(fd, true);
            }
        }
    } while (count > 0);
}

int UCXDriver::init(EventCenter *c, int nevent)
{
    ldout(cct, 20) << __func__ << " num of events: " << nevent << dendl;
    return EpollDriver::init(c, nevent);
}

int UCXDriver::add_event(int fd, int cur_mask, int add_mask)
{
    ldout(cct, 0) << __func__ << " fd: " << fd << " read ? " << (EVENT_READABLE & add_mask) << dendl;

    if (fd_pool.is_system_fd(fd)) {
        return EpollDriver::add_event(fd, cur_mask, add_mask);
    }

    if (EVENT_READABLE & add_mask) {
        assert(!in_set(read_events, fd));
        read_events.insert(fd);

        if (is_connected(fd) &&
                    connections[fd].rx_queue.size() > 0) {
            undelivered.insert(fd);
        }
    }

    if (EVENT_WRITABLE & add_mask) {
        assert(!in_set(write_events, fd));
        write_events.insert(fd);
    }

    return 0;
}

int UCXDriver::del_event(int fd, int cur_mask, int delmask)
{
    ldout(cct, 20) << __func__ << " fd: " << fd << " read ? " << (EVENT_READABLE & delmask) << dendl;

    if (fd_pool.is_system_fd(fd)) {
        return EpollDriver::del_event(fd, cur_mask, delmask);
    }

    if (EVENT_READABLE & delmask) {
        read_events.erase(fd);
        undelivered.erase(fd);
    }

    if (EVENT_WRITABLE & delmask) {
        write_events.erase(fd);
    }

    return 0;
}

int UCXDriver::resize_events(int newsize)
{
    return EpollDriver::resize_events(newsize);
}

int UCXDriver::process_connections(vector<FiredFileEvent> &fired_events)
{
    struct FiredFileEvent fe;

    /* Check for server socket events */
    for (std::set<int>::iterator it = conn_requests.begin();
                                it != conn_requests.end(); ++it) {
        int server_fd = *it;

        if (!accept_queues[server_fd].empty()
                    && read_events.count(server_fd) > 0) {
            struct FiredFileEvent fe;

            fe.fd = server_fd;
            fe.mask = EVENT_READABLE;

            fired_events.push_back(fe);

            ldout(cct, 20) << __func__ << " conn request for server fd: " << server_fd << dendl;
            return fe.fd;
        }
    }

    Mutex::Locker l(lock);
    if (!accepted.empty()) {
        accept_t *c = accepted.front();

        accepted.pop_front();
        if (server_conn_create(c->fd, c->ep_addr) < 0) {
            ceph_abort();
        }

        delete c;

        if (read_events.count(c->fd) > 0) {
            fe.fd = c->fd;
            fe.mask = EVENT_READABLE;
            fired_events.push_back(fe);

            ldout(cct, 20) << __func__ << " accepted fd: " << c->fd << dendl;

            return fe.fd;
        }
    }

    ldout(cct, 20) << __func__ << " nothing to proccess..." << dendl;

    return 0;
}

int UCXDriver::process_writes(vector<FiredFileEvent> &fired_events)
{
    for (std::set<int>::iterator it = write_events.begin();
                                it != write_events.end(); ++it) {
        struct FiredFileEvent fe;

        fe.fd = *it;
        fe.mask = EVENT_WRITABLE;
        fired_events.push_back(fe);

        ldout(cct, 20) << __func__ << " write event on fd: " << fe.fd << dendl;
    }

    return fired_events.size();
}

int UCXDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
    if (NULL == ucp_worker) {
        return EpollDriver::event_wait(fired_events, tvp);
    }

    assert(0 == fired_events.size());
    assert(!connections.empty() || undelivered.empty());

    if (process_connections(fired_events)) {
        return fired_events.size();
    }

    if (!process_writes(fired_events) && undelivered.empty()) {
        vector<FiredFileEvent> events;

        if (UCS_OK == ucp_worker_arm(ucp_worker)) {
            int num_events = EpollDriver::event_wait(events, tvp);
            if (num_events < 0) {
                lderr(cct) << __func__ << " epoll wait failed with err: "
                           << num_events << dendl;

                return num_events;
            }

            ldout(cct, 20) << __func__ << " waking up with " << events.size() << " events " << dendl;

            for (unsigned i = 0; i < events.size(); ++i) {
                struct FiredFileEvent fe = events[i];

                if (ucp_fd != fe.fd) {
                    fired_events.push_back(fe);
                    ldout(cct, 20) << __func__ << " fd: " << fe.fd << " fired event " << dendl;
                }
            }
        }
    }

    event_progress();
    ldout(cct, 20) << __func__ << " undelivered size: " << undelivered.size() << dendl;

    for (std::set<int>::iterator it = undelivered.begin();
                                it != undelivered.end(); ++it) {
        struct FiredFileEvent fe;

        fe.fd = *it;
        fe.mask = EVENT_READABLE;

        assert(fe.fd > 0);

        assert(read_events.count(fe.fd) > 0);
        assert(!connections[fe.fd].rx_queue.empty());

        fired_events.push_back(fe);
        ldout(cct, 20) << __func__ << " fd: " << fe.fd << " undelivered event" << dendl;
    }

    return fired_events.size();
}
