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


UCXConnectedSocketImpl::UCXConnectedSocketImpl(UCXWorker *w, int fd) : worker(w), conn_fd(fd)
{
}

void UCXConnectedSocketImpl::shutdown()
{
    ldout(cct(), 20) << __func__ << " conn fd: "
                     << conn_fd << " is shutting down..." << dendl;

    /* Call for shutdown even ucp_ep doesn't exist yet */
    if (conn_fd) {
        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());

        driver->conn_shutdown(conn_fd);
    }
}

void UCXConnectedSocketImpl::close()
{
    UCXConnectedSocketImpl::shutdown();

    if (conn_fd) {
        ldout(cct(), 20) << __func__ << " fd: " << conn_fd << dendl;

        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
        driver->conn_close(conn_fd);

        conn_fd = 0;
    }
}

UCXConnectedSocketImpl::~UCXConnectedSocketImpl()
{
    UCXConnectedSocketImpl::close();
}

int UCXConnectedSocketImpl::is_connected()
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->is_connected(conn_fd);
}

ssize_t UCXConnectedSocketImpl::read(int fd_or_id, char *buf, size_t len)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->read(conn_fd, buf, len);
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
    return driver->send(conn_fd, bl, more);
}

UCXServerSocketImpl::UCXServerSocketImpl(UCXWorker *w, int server_socket)
                                          : server_fd(server_socket), worker(w)
{
}

UCXServerSocketImpl::~UCXServerSocketImpl()
{
    if (server_fd > 0) {
        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
        driver->stop_listen(server_fd);
    }
}

int UCXServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
    port = sa.get_port();

    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->listen(sa, opt, server_fd);
}

int UCXServerSocketImpl::accept(ConnectedSocket *sock,
                                const SocketOptions &opt,
                                entity_addr_t *out,
                                Worker *w)
{
    int ret;
    ucp_ep_address_t *ep_addr;

    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());

    ret = driver->accept(server_fd, ep_addr);
    if (ret < 0) {
        return -EAGAIN; //Vasily: ??????
    }

    UCXWorker *ucx_worker = (UCXWorker *) w;

    int fd = ucx_worker->get_fd();
    UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(dynamic_cast<UCXWorker *>(w), fd);

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    {
        struct sockaddr_in *addr = (struct sockaddr_in *) malloc(sizeof(*addr));

        memset(addr, 0, sizeof(*addr));

        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = inet_addr("12.210.8.58");

        addr->sin_port = port;
        out->set_sockaddr((sockaddr *)addr);
    }

    ret = ucx_worker->signal(fd, ep_addr);
    if (ret < 0) {
        ceph_abort(); //Vasily: return code ????
    }

    return 0;
}

void UCXServerSocketImpl::abort_accept()
{
    if (server_fd > 0) { //Vasily: ????
        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
        driver->abort_accept(server_fd);
    }

    server_fd = -1;
}

UCXWorker::UCXWorker(CephContext *c, unsigned i) : Worker(c, i)
{
}

UCXWorker::~UCXWorker()
{
}

int UCXWorker::get_fd()
{
   return driver->get_fd();
}

int UCXWorker::signal(int fd, ucp_ep_address_t *ep_addr)
{
   return driver->signal(fd, ep_addr);
}

int UCXWorker::listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *sock)
{
    int server_socket = 0;

    int r = driver->listen(addr, opts, server_socket);
    if (r < 0) {
        return r;
    }

    UCXServerSocketImpl *p = new UCXServerSocketImpl(this, server_socket);

    *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
    return 0;
}

int UCXWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *sock)
{
    int fd = 0;

    if (!stack->is_ready()) {
      ldout(cct, 10) << __func__ << " Network Stack is not ready" << dendl;
      return -1;
    }


    driver = dynamic_cast<UCXDriver *>(center.get_driver());

    int r = driver->connect(addr, opts, fd);
    if (r < 0) {
        ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
        return r;
    }

    UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(this, fd);

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    return 0;
}

void UCXWorker::initialize()
{
}

void UCXWorker::destroy()
{
    driver->cleanup();
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
    params.tag_sender_mask   = -1;
    params.request_size      = sizeof(ucx_req_descr);

    params.request_init      = UCXConnectedSocketImpl::request_init;
    params.request_cleanup   = UCXConnectedSocketImpl::request_cleanup;

    status = ucp_init(&params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);

    if (UCS_OK != status) {
        lderr(cct) << __func__ << "failed to init UCP context" << dendl;
        ceph_abort();
    }

    for (unsigned i = 0; i < get_num_worker(); i++) {
        UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
        w->worker_init();
    }

    ucp_context_print_info(ucp_context, stdout);
}

void UCXWorker::worker_init()
{
    driver = dynamic_cast<UCXDriver *>(center.get_driver());
    driver->worker_init(get_stack()->get_ucp_context());
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
    threads.resize(i + 1);
    threads[i] = std::thread(func);
}

void UCXStack::join_worker(unsigned i)
{
    assert(threads.size() > i && threads[i].joinable());
    threads[i].join();
}
