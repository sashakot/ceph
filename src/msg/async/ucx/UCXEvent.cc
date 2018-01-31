#include "UCXStack.h"
#include "UCXEvent.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "UCXDriver "

int UCXDriver::send_addr(int fd,
                         ucp_address_t *ucp_addr,
                         size_t ucp_addr_len)
{
    int rc;
    ucx_connect_message msg;

    /* Send connected message */
    msg.addr_len = ucp_addr_len; //Vasily: if I need to exchange it ?????

    //Vasily: write in network order
    rc = ::write(fd, &msg, sizeof(msg));
    if (rc != sizeof(msg)) {
        lderr(cct) << __func__ << " failed to send connect msg header" << dendl;
        return -errno;
    }

    rc = ::write(fd, ucp_addr, ucp_addr_len);
    if (rc != (int) ucp_addr_len) {
        lderr(cct) << __func__ << " failed to send worker address " << dendl;
        return -errno;
    }

    ldout(cct, 10) << __func__ << " fd = "
                   << fd << " addr sent successfully  " << dendl;

    return 0;
}

int UCXDriver::conn_establish(int fd,
                              ucp_address_t *ucp_addr,
                              size_t ucp_addr_len)
{
    ldout(cct, 10) << __func__ << " fd = " << fd << " start connecting " << dendl;

    int rc = send_addr(fd, ucp_addr, ucp_addr_len);
    if (rc < 0) {
        return rc;
    }

    //Vasily: could be called by another thread (not worker) from the server side => need use lock for this insertion
    connecting.insert(fd);

    return 0;
}

char *UCXDriver::recv_addr(int fd)
{
    int rc;
    char *addr_buf;

    ucx_connect_message msg;

    // get our peer address
    rc = ::read(fd, &msg, sizeof(msg));
    if (!rc) {
        return NULL;
    }

    if (rc != sizeof(msg)) {
        lderr(cct) << __func__ << " failed to recv connect msg header" << dendl;
        return NULL;
    }

    ldout(cct, 10) << __func__ << " addr len: " << msg.addr_len << dendl;

    addr_buf = new char [msg.addr_len];

    rc = ::read(fd, addr_buf, msg.addr_len);
    if (rc != (int) msg.addr_len) {
        lderr(cct) << __func__ << " failed to recv worker address " << dendl;
        return NULL;
    }

    return addr_buf;
}

int UCXDriver::conn_create(int fd)
{
    ucs_status_t status;
    ucp_ep_params_t params;

    connection_t &conn = connections[fd];

    ldout(cct, 20) << __func__ << " conn for fd = " << fd
                               << " is creating " << dendl;

    char *addr_buf = recv_addr(fd);
    if (!addr_buf) {
        return -EINVAL;
    }

    params.user_data  = reinterpret_cast<void *>(fd);
    params.address    = reinterpret_cast<ucp_address_t *>(addr_buf);

    params.field_mask = UCP_EP_PARAM_FIELD_USER_DATA |
                        UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;

    status = ucp_ep_create(ucp_worker, &params, &conn.ucp_ep);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to create UCP endpoint " << dendl;
        return -EINVAL;
    }

    std::deque<bufferlist*> &pending = conn.pending;

    while (!pending.empty()) {
        bufferlist *bl = pending.front();

        pending.pop_front();
        send(fd, *bl, 0);
    }

    assert(connections[fd].pending.empty());
    delete [] addr_buf; //Vasily: allocate it in this func ????

    connecting.erase(fd);

    waiting_events.insert(fd);
    undelivered += conn.rx_queue.size();

    ldout(cct, 20) << __func__ << " fd = " << fd
                   << " ep=" << (void *)conn.ucp_ep
                   << " connection was successfully created " << dendl;

    return 0;
}

void UCXDriver::conn_close(int fd)
{
    if (!is_connected(fd)) {
        ldout(cct, 20) << __func__ << " UCP ep is NULL, shut the connection down " << dendl;
        return;
    }

    ucs_status_ptr_t request;
    ucp_ep_h ucp_ep = connections[fd].ucp_ep;

    ldout(cct, 20) << __func__ << " fd=" << fd << " ep = " << (void *)ucp_ep
                               << " is shutting down " << dendl;

    request = ucp_ep_close_nb(ucp_ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (NULL == request) {
        ldout(cct, 20) << __func__ << " ucp ep fd=" << fd << " closed in place........." << dendl;
    } else if (UCS_PTR_IS_ERR(request)) {
        lderr(cct) << __func__ << " ucp_ep_close_nb call failed " << dendl;
    } else if (UCS_PTR_STATUS(request) != UCS_OK) {
        /* Wait till the request finalizing */
        do {
            ucp_worker_progress(ucp_worker);
        } while (UCS_INPROGRESS ==
                    ucp_request_check_status(request));

        ucp_request_free(request);
    }

    //Vasily: while (ucp_worker_progress(ucp_worker));
    std::deque<ucx_rx_buf *> &rx_queue = connections[fd].rx_queue;

    /* Free all undelivered receives */
    while (!rx_queue.empty()) {
        ucx_rx_buf *rx_buf = rx_queue.front();

        rx_queue.pop_front();
        delete rx_buf;
    }

    connections.erase(fd);
}

void UCXDriver::addr_create(ucp_context_h ucp_context,
                            ucp_address_t **ucp_addr,
                            size_t *ucp_addr_len)
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

    status = ucp_worker_create(ucp_context, &params, &ucp_worker);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to create UCP worker " << dendl;
        ceph_abort();
    }

    status = ucp_worker_get_address(ucp_worker, ucp_addr, ucp_addr_len);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to obtain UCP worker address " << dendl;
        ceph_abort();
    }

    status = ucp_worker_get_efd(ucp_worker, &ucp_fd);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to obtain UCP worker event fd " << dendl;
        ceph_abort();
    }

    EpollDriver::add_event(ucp_fd, 0, EVENT_READABLE);
}

void UCXDriver::cleanup(ucp_address_t *ucp_addr)
{
    EpollDriver::del_event(ucp_fd, EVENT_READABLE, EVENT_READABLE);

    ucp_worker_release_address(ucp_worker, ucp_addr);
    ucp_worker_destroy(ucp_worker);
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

    if (total_len == 0) {// TODO: shouldn't happen
        return 0;
    }

//Vasily: 'more flag should be taken into account

    if (NULL == connections[fd].ucp_ep) {
        connections[fd].pending.push_back(&bl);
        ldout(cct, 20) << __func__ << " put send to the pending, fd: " << fd << dendl;

        return 0; //return total_len; //Vasily: ?????
    }

    ldout(cct, 20) << __func__ << " fd=" << fd << " sending "
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
        return -1;
    }

    req->bl->claim_append(bl);
    req->iov_list = iov_list;

    ldout(cct, 10) << __func__ << " send in progress req " << req << dendl;

    return 0;
}

void UCXDriver::send_completion_cb(void *req, ucs_status_t status)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    UCXDriver::send_completion(desc);
    ucp_request_free(req);
}

void UCXDriver::insert_rx(int fd, uint8_t *rdata, size_t length)
{
    ucx_rx_buf *rx_buf = new ucx_rx_buf();

    rx_buf->offset = 0;
    rx_buf->rdata  = rdata;
    rx_buf->length = length;

    if (in_set(waiting_events, fd)) {
        ++undelivered;
    }

    connections[fd].rx_queue.push_back(rx_buf);

    ldout(cct, 20) << __func__ << " fd=" << fd << " insert rx buff "
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

int UCXDriver::read(int fd, char *rbuf, size_t bytes)
{
    std::deque<ucx_rx_buf *> &rx_queue = connections[fd].rx_queue;
    if (rx_queue.empty()) {
        return -EAGAIN;
    }

    size_t left = 0;

    ucx_rx_buf *rx_buf = rx_queue.front();
    if (!rx_buf->length) {
        ldout(cct, 20) << __func__ << " fd=" << fd << " Zero length packet..." << dendl;
        goto erase_buf;
    }

    left = rx_buf->length - rx_buf->offset;
    ldout(cct, 20) << __func__ << " read to " << (void *)rbuf << " wanted "
                               << bytes << " left " << left << " rx_buf = "
                               << (void *)rx_buf << " rx_buf->length = "
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

    --undelivered;
    assert(undelivered >= 0);

    return left;
}

void UCXDriver::event_progress(vector<FiredFileEvent> &fired_events)
{
    if (connections.empty()) {
        ldout(cct, 20) << __func__ << " The connections list is empty " << dendl;
        return;
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

            ldout(cct, 20) << __func__ << " Worker poll: count = "
                           << count << " fd=" << fd << " ep="
                           << (void *)poll_eps[i].ep << dendl;

            std::deque<ucx_rx_buf *> &rx_queue = connections[fd].rx_queue;

            recv_stream(fd);

            if (!rx_queue.empty() && in_set(waiting_events, fd)) {
                struct FiredFileEvent fe;

                fe.fd = fd;
                fe.mask = EVENT_READABLE;

                fired_events.push_back(fe);
            }
        }
    } while (count > 0);

    ldout(cct, 20) << __func__ << " Exit from events handler: num of events = "
                               << fired_events.size() << dendl;
}

int UCXDriver::init(EventCenter *c, int nevent)
{
	return EpollDriver::init(c, nevent);
}

int UCXDriver::add_event(int fd, int cur_mask, int add_mask)
{
    ldout(cct, 20) << __func__ << " fd = " << fd << " read ? " << (EVENT_READABLE & add_mask) << dendl;

    if (EVENT_READABLE & add_mask &&
                        is_connected(fd)) {
        assert(!in_set(waiting_events, fd));
        waiting_events.insert(fd);
        undelivered += connections[fd].rx_queue.size();
    }

	return EpollDriver::add_event(fd, cur_mask, add_mask);
}

int UCXDriver::del_event(int fd, int cur_mask, int delmask)
{
    ldout(cct, 20) << __func__ << " fd = " << fd << " read ? " << (EVENT_READABLE & delmask) << dendl;

    if (EVENT_READABLE & delmask &&
                        is_connected(fd)) {
        assert(in_set(waiting_events, fd));
        waiting_events.erase(fd);
        undelivered -= connections[fd].rx_queue.size();
        assert(undelivered >= 0);
    }

	return EpollDriver::del_event(fd, cur_mask, delmask);
}

int UCXDriver::resize_events(int newsize)
{
	return EpollDriver::resize_events(newsize);
}

int UCXDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
    bool ucp_event = false;
    vector<FiredFileEvent> events;

    if (!undelivered && UCS_OK == ucp_worker_arm(ucp_worker)) {
        int num_events = EpollDriver::event_wait(events, tvp);
        if (num_events < 0) {
            return num_events;
        }

        for (unsigned i = 0; i < events.size(); ++i) {
            struct FiredFileEvent fe = events[i];

            if (ucp_fd != fe.fd) {
                if ((EVENT_READABLE & fe.mask) &&
                            (in_set(waiting_events, fe.fd) ||
                                (connecting.count(fe.fd) > 0 &&
                                        conn_create(fe.fd) < 0))) {
                    /* Insert zero message */
                    insert_rx(fe.fd, NULL, 0);
                }

                if (!connections.count(fe.fd) ||
                            in_set(waiting_events, fe.fd)) {
                    ldout(cct, 20) << __func__ << " fd = " << fe.fd << " fired event " << dendl;
                    fired_events.push_back(fe);
                 }
            } else {
                ucp_event = true;
            }
        }
    } else {
        ucp_event = true;
        ldout(cct, 20) << __func__ << " UCP arm is not OK, undelivered = " << undelivered << dendl;
    }

    if (ucp_event) {
        event_progress(fired_events);
    }

    return fired_events.size();
}

