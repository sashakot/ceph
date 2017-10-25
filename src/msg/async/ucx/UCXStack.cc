// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017  Mellanox Technologies Ltd. All rights reserved.
 *
 *
 * Author: Alex Mikheev <alexm@mellanox.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "UCXStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "UCXStack "


UCXConnectedSocketImpl::UCXConnectedSocketImpl(UCXWorker *w) :
  worker(w)
{
}

UCXConnectedSocketImpl::~UCXConnectedSocketImpl()
{
}

int UCXConnectedSocketImpl::is_connected()
{
  lderr(cct()) << __func__ << " is " << (tcp_fd > 0) << dendl;
  return tcp_fd > 0;
}

// TODO: consider doing a completely non blockig connect/accept
//do blocking connect
int UCXConnectedSocketImpl::connect(const entity_addr_t& peer_addr, const SocketOptions &opts)
{
    NetHandler net(cct());
    int ret;

    lderr(cct()) << __func__ << dendl;

    tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);
    if (tcp_fd < 0) {
        return -errno;
    }

    ldout(cct(), 20) << __func__ << " tcp_fd: " << tcp_fd << dendl;
    net.set_close_on_exec(tcp_fd);

    ret = net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
    if (ret < 0) {
        lderr(cct()) << __func__ << " failed to set socket options" << dendl;
        goto err;
    }

    net.set_priority(tcp_fd, opts.priority, peer_addr.get_family());

    ret = worker->send_addr(tcp_fd, reinterpret_cast<uint64_t>(this));
    if (ret != 0)
        goto err;

    ret = worker->recv_addr(tcp_fd, &ucp_ep, &dst_tag);
    if (ret != 0)
        goto err;

    worker->add_conn(this);

    return 0;
err:
    ::close(tcp_fd);
    tcp_fd = -1;

    return ret;
}

// do blocking accept()
int UCXConnectedSocketImpl::accept(int server_sock, entity_addr_t *out, const SocketOptions &opt)
{
    NetHandler net(cct());
    int ret = 0;

    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);

    lderr(cct()) << __func__ << " 3 " << dendl;

    tcp_fd = ::accept(server_sock, (sockaddr*)&ss, &slen);
    if (tcp_fd < 0) {
        return -errno;
    }

    net.set_close_on_exec(tcp_fd);

    lderr(cct()) << __func__ << " 4 " << dendl;
    ret = net.set_socket_options(tcp_fd, opt.nodelay, opt.rcbuf_size);
    if (ret < 0) {
        ret = -errno;
        goto err;
    }

    lderr(cct()) << __func__ << " 2 " << dendl;
    assert(NULL != out); //out should not be NULL in accept connection

    out->set_sockaddr((sockaddr*)&ss);
    net.set_priority(tcp_fd, opt.priority, out->get_family());

    lderr(cct()) << __func__ << " 1 " << dendl;

    ret = worker->recv_addr(tcp_fd, &ucp_ep, &dst_tag);
    if (ret != 0)
        goto err;

    lderr(cct()) << __func__ << " ADDRESS RECVD" << dendl;

    ret = worker->send_addr(tcp_fd, reinterpret_cast<uint64_t>(this));
    if (ret != 0)
        goto err;

    worker->add_conn(this);
    lderr(cct()) << __func__ << " ADDRESS SENT" << dendl;

    return 0;
err:
    ::close(tcp_fd);
    tcp_fd = -1;
    lderr(cct()) << __func__ << " failed accept " << ret << dendl;

    return ret;
}

ssize_t UCXConnectedSocketImpl::read(int fd_or_id, char *buf, size_t len)
{
    if (fd_or_id == tcp_fd) {
        assert(fd_or_id > 0);
        return ::read(tcp_fd, buf, len);
    }

    if (rx_queue.empty())
        return -EAGAIN;

    size_t left;
    ucx_rx_buf *rx_buf;

    rx_buf = rx_queue.front();
    left = rx_buf->length - rx_buf->offset;

    ldout(cct(), 20) << __func__ << " read to " << buf << " wanted " <<
                             len << " left " << left << dendl;

    if (len < left) {
        memcpy(buf, rx_buf->data, len);
        rx_buf->offset += len;
        return len;
    }
    // TODO: copy more data
    memcpy(buf, rx_buf->data, left);
    rx_queue.pop_front();
    free(rx_buf);

    return left;
}

ssize_t UCXConnectedSocketImpl::zero_copy_read(bufferptr&)
{
  lderr(cct()) << __func__ << dendl;
  return 0;
}

void UCXConnectedSocketImpl::send_completion_cb(void *req, ucs_status_t status)
{
  ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

  desc->conn->send_completion(desc);
  lderr(desc->conn->cct()) << __func__ << " completed send request " << req << dendl;
  //ucp_request_free(req);
  desc->conn = NULL;
  ucp_request_release(req);
}

void UCXConnectedSocketImpl::dispatch_rx(ucx_rx_buf *buf)
{
  //queue
  buf->offset = 0;
  rx_queue.push_back(buf);
  // todo kick read
  if (read_progress)
    read_progress->do_request(worker->fd());
}

void UCXConnectedSocketImpl::recv_completion_cb(void *req, ucs_status_t status,
                                ucp_tag_recv_info_t *info)
{
  ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

  if (desc->conn) {
    lderr(desc->conn->cct()) << __func__ << " completed recv request " << req << dendl;
    desc->conn->dispatch_rx(desc->rx_buf);
  }
  ucp_request_release(req);
}

void UCXConnectedSocketImpl::request_init(void *req)
{
  ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);
  desc->conn = NULL;
  desc->bl = new bufferlist;
}

void UCXConnectedSocketImpl::request_cleanup(void *req)
{
  ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

  delete desc->bl;
}


ssize_t UCXConnectedSocketImpl::send(bufferlist &bl, bool more)
{
  unsigned iov_cnt = bl.get_num_buffers();
  unsigned total_len = bl.length();
  ucx_req_descr *req;
  int n;
  ucp_dt_iov_t *iov_list;
  char ll[256];

  if (total_len == 0)  // TODO: shouldnt happen
    return 0;

  ldout(cct(), 15) << __func__ << " sending " << total_len <<
    " bytes. iov_cnt " << iov_cnt << " to " << dst_tag << dendl;

  std::list<bufferptr>::const_iterator i = bl.buffers().begin();
  if (iov_cnt == 1) {
    iov_list = 0;
    req = static_cast<ucx_req_descr *>(ucp_tag_send_nb(ucp_ep,
          i->c_str(), i->length(), ucp_dt_make_contig(1),
          dst_tag, send_completion_cb));
  }
  else {
    n = 0;
    // TODO: pool alloc, or ucx optimization...
    iov_list = new ucp_dt_iov_t[iov_cnt];
    for (n = 0; i != bl.buffers().end(); ++i, n++) {
      iov_list[n].buffer = reinterpret_cast<void *>(const_cast<char *>(i->c_str()));
      iov_list[n].length = i->length();
      snprintf(ll, sizeof(ll), "iov %d: %p len %lu", n, iov_list[n].buffer, iov_list[n].length);
      ldout(cct(), 15) << __func__ << " " << ll << dendl;
    }

    req = static_cast<ucx_req_descr *>(ucp_tag_send_nb(ucp_ep, iov_list, iov_cnt, ucp_dt_make_iov(),
          dst_tag, send_completion_cb));
  }

  if (req == NULL) {
    /* in place completion */
    ldout(cct(), 20) << __func__ << " SENT IN PLACE " << dendl;
    bl.clear();
    return 0;
  }
  if (UCS_PTR_IS_ERR(req)) {
    return -1;
  }

  req->conn = this;
  req->bl->claim_append(bl);
  req->iov_list = iov_list;
  ldout(cct(), 20) << __func__ << " send in progress req " << req << dendl;

  return 0;
}

void UCXConnectedSocketImpl::shutdown()
{
    ucs_status_ptr_t request;

    lderr(cct()) << __func__ << dendl;
    worker->remove_conn(this);

    /* TODO: free request */
    request = ucp_ep_close_nb(ucp_ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_ERR(request)) {
         lderr(cct()) << __func__ << " ucp_ep_close_nb call failed " << dendl;
    } else if (UCS_PTR_STATUS(request) != UCS_OK) {
        /* Wait till the request finalizing */
        ucs_status_t status;

        do {
            worker->ucp_progress();
            status = ucp_request_test(request, NULL);
        } while (status == UCS_INPROGRESS);

        ucp_request_free(request);
    }

    ::close(tcp_fd);
    tcp_fd = -1;
}

void UCXConnectedSocketImpl::close()
{
    lderr(cct()) << __func__ << dendl;
}

UCXServerSocketImpl::UCXServerSocketImpl(UCXWorker *w) :
  worker(w)
{
}

UCXServerSocketImpl::~UCXServerSocketImpl()
{
  if (tcp_fd >= 0)
    ::close(tcp_fd);
}

int UCXServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
  NetHandler net(cct());
  int rc;

  tcp_fd = net.create_socket(sa.get_family(), true);
  if (tcp_fd < 0) {
    lderr(cct()) << __func__ << " failed to create server socket: "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  rc = net.set_nonblock(tcp_fd);
  if (rc < 0) {
    goto err;
  }

  net.set_close_on_exec(tcp_fd);

  rc = ::bind(tcp_fd, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (rc < 0) {
    ldout(cct(), 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  rc = ::listen(tcp_fd, 128);
  if (rc < 0) {
    lderr(cct()) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  ldout(cct(), 20) << __func__ << " bind to " << sa.get_sockaddr() << " on port " << sa.get_port()  << dendl;
  return 0;

err:
  ::close(tcp_fd);
  tcp_fd = -1;
  return -errno;
}

int UCXServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w)
{
  UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(dynamic_cast<UCXWorker *>(w));

  int r = p->accept(tcp_fd, out, opt);
  if (r < 0) {
    ldout(cct(), 1) << __func__ << " accept failed. ret = " << r << dendl;
    delete p;
    return r;
  }
  std::unique_ptr<UCXConnectedSocketImpl> csi(p);
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void UCXServerSocketImpl::abort_accept()
{
  if (tcp_fd >= 0)
    return;
  ::close(tcp_fd);
  tcp_fd = -1;
}

UCXWorker::UCXWorker(CephContext *c, unsigned i) :
  Worker(c, i), progress_cb(new C_handle_worker_progress(this))
{

}

UCXWorker::~UCXWorker()
{
  delete progress_cb;
}

int UCXWorker::listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *sock)
{
  UCXServerSocketImpl *p = new UCXServerSocketImpl(this);

  int r = p->listen(addr, opts);
  if (r < 0) {
    delete p;
    return r;
  }

  *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
  return 0;
}

int UCXWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *sock)
{
    UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(this);

    int r = p->connect(addr, opts);
    if (r < 0) {
        ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
        delete p;

        return r;
    }

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    return 0;
}

void UCXWorker::dispatch_rx()
{
	ucp_tag_message_h msg;
	ucp_tag_recv_info_t msg_info;
	ucx_rx_buf *rx_buf;
	ucx_req_descr *req;

    UCXConnectedSocketImpl *conn;

    if (connections.empty()) { //Vasily: whether it's possible ?????????????????????????????
        return;
    }

    std::list<UCXConnectedSocketImpl*>::iterator iterator;
    for (iterator = connections.begin(); iterator != connections.end(); ++iterator) {
        UCXConnectedSocketImpl *conn = *iterator;

        uint64_t tag = reinterpret_cast<uint64_t>(conn);
        msg = ucp_tag_probe_nb(ucp_worker, tag, -1, 1, &msg_info);
        if (msg == NULL)
            continue;

        assert(tag == msg_info.sender_tag);

        rx_buf = (ucx_rx_buf *) malloc(sizeof(*rx_buf) + msg_info.length);
        ldout(cct, 20) << __func__ << " message on socket " << msg_info.sender_tag << " len " << msg_info.length << dendl;

        rx_buf->length = msg_info.length;
        req = reinterpret_cast<ucx_req_descr *>(
                        ucp_tag_msg_recv_nb(
                                    ucp_worker,
                                    rx_buf->data,
                                    rx_buf->length,
                                    ucp_dt_make_contig(1),
                                    msg,
                                    UCXConnectedSocketImpl::recv_completion_cb));
        if (UCS_PTR_IS_ERR(req)) {
            lderr(cct) << __func__ << " FAILED to rx message socket " << msg_info.sender_tag << " len " << msg_info.length << dendl;
            return;
        }

        if (ucp_request_test(req, &msg_info) == UCS_INPROGRESS) {
            req->rx_buf = rx_buf;
            req->conn = conn;
        } else {
            ldout(cct, 20) << __func__ << " rx completion in place " << dendl;
            conn->dispatch_rx(rx_buf);
        }
    }
}

void UCXWorker::recv_progress()
{
    ucp_progress();
    dispatch_rx();
}

void UCXWorker::ucp_progress()
{
    int ev_count = 0;
    ucs_status_t status;

    do {
        ucp_worker_progress(ucp_worker);

        status = ucp_worker_arm(ucp_worker);
        ev_count++;
    } while (UCS_OK != status);

    ldout(cct, 20) << __func__ << " handler " << ev_count << " events " << dendl;
}

void UCXWorker::initialize()
{
  ucs_status_t status;
  ucp_worker_params_t params;

  params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  // TODO: check if we need a multi threaded mode
  params.thread_mode = UCS_THREAD_MODE_SINGLE;

  status = ucp_worker_create(get_stack()->get_ucp_context(), &params, &ucp_worker);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to create UCP worker " << dendl;
    ceph_abort();
  }
  if (id == 0)
    ucp_worker_print_info(ucp_worker, stdout);

  status = ucp_worker_get_address(ucp_worker, &ucp_addr, &ucp_addr_len);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to obtain worker address " << dendl;
    ceph_abort();
  }

  // add event fd
  status = ucp_worker_get_efd(ucp_worker, &ucp_fd);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to obtain worker event fd " << dendl;
    ceph_abort();
  }
  center.create_file_event(ucp_fd, EVENT_READABLE, progress_cb);
  recv_progress();
}

void UCXWorker::destroy()
{
  ucp_worker_release_address(ucp_worker, ucp_addr);
  center.delete_file_event(ucp_fd, EVENT_READABLE);
  ucp_worker_destroy(ucp_worker);
}

void UCXWorker::set_stack(UCXStack *s)
{
  stack = s;
}

int UCXWorker::send_addr(int sock, uint64_t tag)
{
  ucx_connect_message msg;
  int rc;

  // send connected message
  msg.addr_len = ucp_addr_len;
  msg.tag      = tag;

  rc = ::write(sock, &msg, sizeof(msg));
  if (rc != sizeof(msg)) {
    lderr(cct) << __func__ << " failed to send connect msg header" << dendl;
    return -errno;
  }

  rc = ::write(sock, ucp_addr, ucp_addr_len);
  if (rc != (int)ucp_addr_len) {
    lderr(cct) << __func__ << " failed to send worker address " << dendl;
    return -errno;
  }

  return 0;
}

int UCXWorker::recv_addr(int sock, ucp_ep_h *ep, uint64_t *dst_tag)
{
  ucx_connect_message msg;
  int rc, ret = 0;
  char *addr_buf;
  ucs_status_t status;
  ucp_ep_params_t params;

  // get our peer address
  rc = ::read(sock, &msg, sizeof(msg));
  if (rc != sizeof(msg)) {
    lderr(cct) << __func__ << " failed to recv connect msg header" << dendl;
    return -errno;
  }

  ldout(cct, 1) << __func__ << " received tag: " << msg.tag <<
                " addr len: " << msg.addr_len << dendl;

  *dst_tag = msg.tag;
  addr_buf = new char [msg.addr_len];
  rc = ::read(sock, addr_buf, ucp_addr_len);
  if (rc != (int)ucp_addr_len) {
    lderr(cct) << __func__ << " failed to recv worker address " << dendl;
    ret = -errno;
    goto out;
  }

  params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  params.address    = reinterpret_cast<ucp_address_t *>(addr_buf);
  status = ucp_ep_create(ucp_worker, &params, ep);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to create UCP endpoint " << dendl;
    ret = -EINVAL;
  }

out:
  delete [] addr_buf;
  return ret;
}

UCXStack::UCXStack(CephContext *cct, const string &t) :
   NetworkStack(cct, t)
{

    ucs_status_t status;
    ucp_config_t *ucp_config;
    ucp_params_t params;

    ldout(cct, 0) << __func__ << " constructing UCX stack " << t
                  << " with " << get_num_worker() << " workers " << dendl;

    int rc = setenv("UCX_CEPH_NET_DEVICES", cct->_conf->ms_async_ucx_device.c_str(), 1);
    if (rc) {
        lderr(cct) << __func__ << " failed to export UCX_CEPH_NET_DEVICES. Application aborts." << dendl;
        ceph_abort();
    }

    rc = setenv("UCX_CEPH_TLS", cct->_conf->ms_async_ucx_tls.c_str(), 1);
    if (rc) {
        lderr(cct) << __func__ << " failed to export UCX_CEPH_TLS. Application aborts." << dendl;
        ceph_abort();
    }

    status = ucp_config_read("CEPH", NULL, &ucp_config);
    if (UCS_OK != status) {
      lderr(cct) << __func__ << "failed to read UCP config" << dendl;
      ceph_abort();
    }

    memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES|
                        UCP_PARAM_FIELD_REQUEST_SIZE|
                        UCP_PARAM_FIELD_REQUEST_INIT|
                        UCP_PARAM_FIELD_REQUEST_CLEANUP|
                        UCP_PARAM_FIELD_TAG_SENDER_MASK|
                        UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    params.features   = UCP_FEATURE_TAG|UCP_FEATURE_WAKEUP;
    params.mt_workers_shared = 1;
    params.tag_sender_mask = -1;
    params.request_size    = sizeof(ucx_req_descr);
    params.request_init    = UCXConnectedSocketImpl::request_init;
    params.request_cleanup = UCXConnectedSocketImpl::request_cleanup;

    status = ucp_init(&params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);
    if (UCS_OK != status) {
      lderr(cct) << __func__ << "failed to init UCP context" << dendl;
      ceph_abort();
    }
    ucp_context_print_info(ucp_context, stdout);

    for (unsigned i = 0; i < get_num_worker(); i++) {
      UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
      w->set_stack(this);
    }
}

UCXStack::~UCXStack()
{
  ucp_cleanup(ucp_context);
}

void UCXStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
  threads.resize(i+1);
  threads[i] = std::thread(func);
}

void UCXStack::join_worker(unsigned i)
{
  assert(threads.size() > i && threads[i].joinable());
  threads[i].join();
}
