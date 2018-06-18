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
#include "UCXEvent.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "UCXStack "


UCXConnectedSocketImpl::UCXConnectedSocketImpl(UCXWorker *w) : worker(w)
{
}

void UCXConnectedSocketImpl::shutdown()
{
    ldout(cct(), 20) << __func__ << " conn fd: "
                     << tcp_fd << " is shutting down..." << dendl;

    /* Call for shutdown even ucp_ep doesn't exist yet */
    if (tcp_fd > 0) {
        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
       //::shutdown(tcp_fd, SHUT_RDWR); //Vasily: ????
        driver->conn_shutdown(tcp_fd);
    }
}

void UCXConnectedSocketImpl::close()
{
    UCXConnectedSocketImpl::shutdown();

    if (tcp_fd > 0) {
        ldout(cct(), 20) << __func__ << " fd: " << tcp_fd << dendl;

        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
        driver->conn_close(tcp_fd);

        ::close(tcp_fd);
        tcp_fd = -1;
    }
}

UCXConnectedSocketImpl::~UCXConnectedSocketImpl()
{
    UCXConnectedSocketImpl::close();
}

int UCXConnectedSocketImpl::is_connected()
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->is_connected(tcp_fd);
}

int UCXConnectedSocketImpl::connect(const entity_addr_t& peer_addr, const SocketOptions &opts)
{
    NetHandler net(cct());
    int ret;

    tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);
    if (tcp_fd < 0) {
        lderr(cct()) << __func__ << " failed to allocate client socket" << dendl;
        return -errno;
    }

    ldout(cct(), 20) << __func__<< " server addr: " << peer_addr << " tcp_fd: " << tcp_fd << dendl;
    net.set_close_on_exec(tcp_fd);

    ret = net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
    if (ret < 0) {
        ::close(tcp_fd);
        tcp_fd = -1;
        lderr(cct()) << __func__ << " failed to set socket options" << dendl;
        return -errno;
    }

    net.set_priority(tcp_fd, opts.priority, peer_addr.get_family()); //Vasily: ??????

    ret = worker->conn_establish(tcp_fd);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

int UCXConnectedSocketImpl::accept(int server_sock,
                                   entity_addr_t *out,
                                   const SocketOptions &opt)
{
    NetHandler net(cct());
    int ret = 0;

    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);

    tcp_fd = ::accept(server_sock, (sockaddr*)&ss, &slen);
    if (tcp_fd < 0) {
        return -errno;
    }

    net.set_close_on_exec(tcp_fd);

    ret = net.set_socket_options(tcp_fd, opt.nodelay, opt.rcbuf_size);
    if (ret < 0) {
        ::close(tcp_fd);
        tcp_fd = -1;

        return -errno;
    }

    assert(NULL != out); //out should not be NULL in accept connection

    out->set_sockaddr((sockaddr*)&ss);
    net.set_priority(tcp_fd, opt.priority, out->get_family());

    ret = worker->conn_establish(tcp_fd);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

int UCXWorker::conn_establish(int fd)
{
    return  driver->conn_establish(fd, ucp_addr, ucp_addr_len);
}

ssize_t UCXConnectedSocketImpl::read(int fd_or_id, char *buf, size_t len)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->read(tcp_fd, buf, len);
}

ssize_t UCXConnectedSocketImpl::zero_copy_read(bufferptr&)
{
    lderr(cct()) << __func__ << dendl;
    return 0;
}

void UCXConnectedSocketImpl::request_init(void *req)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    desc->rx_buf = NULL;
    desc->bl = new bufferlist;
}

void UCXConnectedSocketImpl::request_cleanup(void *req)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    delete desc->bl;
}

ssize_t UCXConnectedSocketImpl::send(bufferlist &bl, bool more)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->send(tcp_fd, bl, more);
}

UCXServerSocketImpl::UCXServerSocketImpl(UCXWorker *w) : worker(w)
{
}

UCXServerSocketImpl::~UCXServerSocketImpl()
{
    if (server_setup_socket >= 0) {
        ::close(server_setup_socket);
    }
}

int UCXServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
    int rc;
    NetHandler net(cct());

    server_setup_socket = net.create_socket(sa.get_family(), true);
    if (server_setup_socket < 0) {
        ldout(cct(), 1)  << __func__ << " failed to create server socket: "
                         << cpp_strerror(errno) << dendl;
        return -errno;
    }

    rc = net.set_nonblock(server_setup_socket);
    if (rc < 0) {
        goto err;
    }

    net.set_close_on_exec(server_setup_socket);

    rc = ::bind(server_setup_socket, sa.get_sockaddr(), sa.get_sockaddr_len());
    if (rc < 0) {
        ldout(cct(), 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                         << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
        goto err;
    }

    rc = ::listen(server_setup_socket, 128);
    if (rc < 0) {
        lderr(cct()) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(errno) << dendl;
        goto err;
    }

    ldout(cct(), 20) << __func__ << " bind server addr " << sa
                     << " to server socket " << server_setup_socket << dendl;

    return 0;

err:
    ::close(server_setup_socket);
    server_setup_socket = -1;

    return -errno;
}

int UCXServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w)
{
    UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(dynamic_cast<UCXWorker *>(w));

    int r = p->accept(server_setup_socket, out, opt);
    if (r < 0) {
        ldout(cct(), 1) << __func__ << " accept failed on server socket: "
                        << server_setup_socket << dendl;
        delete p;

        return r;
    }

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    return 0;
}

void UCXServerSocketImpl::abort_accept()
{
    if (server_setup_socket >= 0) {
        ::close(server_setup_socket);
    }

    server_setup_socket = -1;
}

UCXWorker::UCXWorker(CephContext *c, unsigned i) : Worker(c, i)
{
}

UCXWorker::~UCXWorker()
{
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

void UCXWorker::initialize()
{
}

void UCXWorker::destroy()
{
    if (NULL != ucp_addr) {
        driver->cleanup(ucp_addr);
        ucp_addr = NULL;
    }
}

void UCXWorker::set_stack(UCXStack *s)
{
    stack = s;
}

void UCXStack::ucx_contex_create()
{
    ucp_params_t params;
    ucs_status_t status;

    ucp_config_t *ucp_config;

    if (NULL != ucp_context) {
        return;
    }

    ldout(cct, 10) << __func__ << " UCX contex is going to be created..." << dendl;

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
    params.field_mask = UCP_PARAM_FIELD_FEATURES        |
                        UCP_PARAM_FIELD_REQUEST_SIZE    |
                        UCP_PARAM_FIELD_REQUEST_INIT    |
                        UCP_PARAM_FIELD_REQUEST_CLEANUP |
                        UCP_PARAM_FIELD_TAG_SENDER_MASK |
                        UCP_PARAM_FIELD_MT_WORKERS_SHARED;

    params.features   = UCP_FEATURE_WAKEUP |
                        UCP_FEATURE_STREAM;

    params.mt_workers_shared = 1;
    params.tag_sender_mask = -1;
    params.request_size    = sizeof(ucx_req_descr);
    params.request_init    = UCXConnectedSocketImpl::request_init;
    params.request_cleanup = UCXConnectedSocketImpl::request_cleanup;

    status = ucp_init(&params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);

    if (UCS_OK != status) {
	
        lderr(cct) << __func__ << " failed to init UCP context: " << ucs_status_string(status) << dendl;
        ceph_abort();
    }

    for (unsigned i = 0; i < get_num_worker(); i++) {
        UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
        w->addr_create();
    }

    ucp_context_print_info(ucp_context, stdout);
}

void UCXWorker::addr_create()
{
    driver = dynamic_cast<UCXDriver *>(center.get_driver());
    driver->addr_create(get_stack()->get_ucp_context(),
                        &ucp_addr, &ucp_addr_len);
}

UCXStack::UCXStack(CephContext *cct, const string &t) : NetworkStack(cct, t)
{
    ldout(cct, 10) << __func__ << " constructing UCX stack " << t
               << " with " << get_num_worker() << " workers " << dendl;

    for (unsigned i = 0; i < get_num_worker(); i++) {
        UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
        w->set_stack(this);
    }
}

UCXStack::~UCXStack()
{
    if (NULL != ucp_context) {
        ucp_cleanup(ucp_context);
    }
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
