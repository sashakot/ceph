#include "UCXStack.h"
#include "UCXEvent.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "UCXDriver "

int UCXDriver::conn_create(int fd,
                           ucp_ep_h *ep,
                           ucp_address_t *ucp_addr)
{
    ucs_status_t status;
    ucp_ep_params_t params;

    ldout(cct, 0) << __func__ << " conn for fd = " << fd
                               << " is opening " << dendl;

    params.address    = ucp_addr;
    params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;

    status = ucp_ep_create(ucp_worker, &params, ep);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << " failed to create UCP endpoint " << dendl;
        return -EINVAL;
    }

    connections.insert(fd);
    //std::deque<ucx_rx_buf *> rx_queue = new std::deque<ucx_rx_buf *>();
    //queues[fd] = rx_queue; //Vasily: ??????

    return 0;
}

const ucp_generic_dt_ops_t DummyDataType::dummy_datatype_ops = {
    .start_pack   = (void* (*)(void*, const void*, size_t)) DummyDataType::dummy_start_cb,
    .start_unpack = DummyDataType::dummy_start_cb,
    .packed_size  = DummyDataType::dummy_datatype_packed_size,
    .pack         = DummyDataType::dummy_pack_cb,
    .unpack       = DummyDataType::dummy_unpack_cb,
    .finish       = DummyDataType::dummy_datatype_finish
};

void UCXDriver::conn_close(int fd, ucp_ep_h ucp_ep)
{
    ucs_status_ptr_t request;

    connections.erase(fd);
    ldout(cct, 0) << __func__ << " conn = " << (void *)ucp_ep
                               << " is shutting down " << dendl;

    request = ucp_ep_close_nb(ucp_ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_ERR(request)) {
         lderr(cct) << __func__ << " ucp_ep_close_nb call failed " << dendl;
    } else if (UCS_PTR_STATUS(request) != UCS_OK) {
        /* Wait till the request finalizing */
        do {
            ucp_worker_progress(ucp_worker);
        } while (UCS_INPROGRESS ==
                    ucp_request_check_status(request));

        ucp_request_free(request);
    }

    std::deque<ucx_rx_buf *> rx_queue = queues[fd];

    /* Free all undelivered receives */
    while (!rx_queue.empty()) {
        ucx_rx_buf *rx_buf = rx_queue.front();

        rx_queue.pop_front();
        free(rx_buf);
    }

    queues.erase(fd);
    //Vasily: delete rx_queue;
}

void UCXDriver::drop_events(int fd)
{
    do {
        ucx_req_descr *req;
        ucp_tag_message_h msg;

        ucp_tag_recv_info_t msg_info;
        uint64_t tag = static_cast<uint64_t>(fd);

        msg = ucp_tag_probe_nb(ucp_worker, tag, -1, 1, &msg_info);
        if (NULL == msg) {
            break;
        }

        req = reinterpret_cast<ucx_req_descr *>(
                        ucp_tag_msg_recv_nb(
                                    ucp_worker, NULL,
                                    1, dummy_dtype.ucp_datatype,
                                    msg, DummyDataType::dummy_completion_cb));

         while (UCS_INPROGRESS ==
                    ucp_request_test(req, &msg_info)) {
            ucp_worker_progress(ucp_worker);
        }
    } while (true);
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

void UCXDriver::recv_completion_cb(void *req, ucs_status_t status,
                                   ucp_tag_recv_info_t *info)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    if (desc->rx_buf) {
        UCXDriver::dispatch_rx(desc->rx_buf, desc->rx_queue);
        desc->rx_buf = NULL;
    }

    ucp_request_free(req);
}

void UCXDriver::insert_zero_msg(int fd)
{
    ucx_rx_buf *rx_buf = (ucx_rx_buf *) malloc(sizeof(*rx_buf));

    rx_buf->length = 0;
    UCXDriver::dispatch_rx(rx_buf, queues[fd]);
}

void UCXDriver::recv_msg(int fd, ucp_tag_message_h msg,
                         ucp_tag_recv_info_t &msg_info)
{
    ucx_rx_buf *rx_buf = (ucx_rx_buf *) malloc(sizeof(*rx_buf) + msg_info.length);
    ldout(cct, 0) << __func__ << " message on fd = " << fd << " len " << msg_info.length << dendl;

    rx_buf->length = msg_info.length;
    assert(msg_info.length > 0);

    ucx_req_descr *req =
          reinterpret_cast<ucx_req_descr *>(
                    ucp_tag_msg_recv_nb(
                                ucp_worker,
                                rx_buf->data,
                                rx_buf->length,
                                ucp_dt_make_contig(1),
                                msg,
                                UCXDriver::recv_completion_cb));
    if (UCS_PTR_IS_ERR(req)) {
        lderr(cct) << __func__ << " FAILED to rx message socket "
                               << msg_info.sender_tag
                               << " length = " << msg_info.length << dendl;
        return;
    }

    if (ucp_tag_recv_request_test(req, &msg_info) == UCS_INPROGRESS) {
        req->rx_buf = rx_buf;
        req->rx_queue = queues[fd];

        ldout(cct, 0) << __func__ << " req = " << (void *)req << " rx in progress: rx_buf = "
                                                << (void *)rx_buf << " rx_buf->length = "
                                                << rx_buf->length << " fd = " << fd << dendl;
    } else {
        ldout(cct, 0) << __func__ << " req = " << (void *)req << " rx completion in place: rx_buf = "
                                                << (void *)rx_buf << " rx_buf->length = "
                                                << rx_buf->length << " fd = " << fd << dendl;
        UCXDriver::dispatch_rx(rx_buf, queues[fd]);
    }
}

ucx_rx_buf *UCXDriver::get_rx_buf(int fd)
{
    if (queues[fd].empty()) {
        ldout(cct, 0) << __func__ << " queue of fd = " << fd << " is empty " << dendl;
        return NULL;
    }

    ldout(cct, 0) << __func__ << " return recv message for fd = " << fd << dendl;
    return queues[fd].front();
}

void UCXDriver::pop_rx_buf(int fd)
{
    if (!queues[fd].empty()) {
        ucx_rx_buf *rx_buf = queues[fd].front();

        queues[fd].pop_front();
        free(rx_buf);
    }
}

void UCXDriver::dispatch_rx(ucx_rx_buf *buf,
                            std::deque<ucx_rx_buf *> &rx_queue)
{
    buf->offset = 0;
    rx_queue.push_back(buf);
}

void UCXDriver::dispatch_events(vector<FiredFileEvent> &fired_events)
{
	ucp_tag_message_h msg;
	ucp_tag_recv_info_t msg_info;

    if (connections.empty()) {
        ldout(cct, 0) << __func__ << " The connections list is empty " << dendl;
        return;
    }

    std::set<int> fds;
    for (std::set<int>::iterator it = connections.begin(); it != connections.end(); ++it) {
        uint64_t tag = static_cast<uint64_t>(*it);

        ldout(cct, 0) << __func__ << " Trying recv for fd = " << *it << dendl;

        while (true) {
            msg = ucp_tag_probe_nb(ucp_worker, tag, -1, 1, &msg_info);
            if (NULL == msg) {
                break;
            }

            recv_msg(*it, msg, msg_info);
            fds.insert(*it);
        }
    }

    for (std::set<int>::iterator it = fds.begin(); it != fds.end(); ++it) {
        struct FiredFileEvent fe;

        fe.fd = (*it);
        fe.mask = EVENT_READABLE;

        fired_events.push_back(fe);
    }
}

void UCXDriver::event_progress(vector<FiredFileEvent> &fired_events)
{
    /*
     * Look at 'ucp_worker_arm' usage example (ucp.h).
     * All existing events must be drained before waiting
     * on the file descriptor, this can be achieved by calling
     * 'ucp_worker_progress' repeatedly until it returns 0.
     */
    while (ucp_worker_progress(ucp_worker));

    dispatch_events(fired_events);
    ldout(cct, 0) << __func__ << " Exit from events handler" << dendl;
}

int UCXDriver::init(EventCenter *c, int nevent)
{
	return EpollDriver::init(c, nevent);
}

int UCXDriver::add_event(int fd, int cur_mask, int add_mask)
{
    ldout(cct, 0) << __func__ << " Add fd = " << fd << dendl;

    //connections.insert(fd);
	return EpollDriver::add_event(fd, cur_mask, add_mask);
}

int UCXDriver::del_event(int fd, int cur_mask, int delmask)
{
    ldout(cct, 0) << __func__ << " Del fd = " << fd << dendl;

    //connections.erase(fd);
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

    if (UCS_OK == ucp_worker_arm(ucp_worker)) {
        int numevents = EpollDriver::event_wait(events, tvp);
        if (numevents < 0) {
            return numevents;
        }

        for (unsigned i = 0; i < events.size(); ++i) {
            struct FiredFileEvent fe = events[i];

            if (ucp_fd != fe.fd) {
                fired_events.push_back(fe);
                if (EVENT_READABLE & fe.mask) {
                    insert_zero_msg(fe.fd);
                }
            } else {
                ucp_event = true;
            }
        }
    } else {
        ucp_event = true;
    }

    if (ucp_event) {
        events.clear();
        event_progress(events);

        for (unsigned i = 0; i < events.size(); ++i) {
            fired_events.push_back(events[i]);
            ldout(cct, 0) << __func__ << " ucp event on fd = " << events[i].fd << dendl;
        }
    }

    return fired_events.size();
}

