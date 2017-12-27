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

#ifndef CEPH_MSG_UCXSTACK_H
#define CEPH_MSG_UCXSTACK_H

#include <vector>
#include <thread>
#include <deque>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"

#include "msg/async/Stack.h"
#include "UCXEvent.h"

extern "C" {
#include <ucp/api/ucp.h>
};

class UCXStack;
class UCXConnectedSocketImpl;

struct ucx_connect_message {
  uint64_t tag;
  uint16_t addr_len;
} __attribute__ ((packed));

class UCXWorker : public Worker {
    UCXStack *stack;
    ucp_address_t *ucp_addr;
    size_t ucp_addr_len;

    UCXDriver *driver;
    // pass received messages to socket(s)
    void dispatch_rx();

public:
    explicit UCXWorker(CephContext *c, unsigned i);
    virtual ~UCXWorker();

    virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
    virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;

    virtual void initialize() override;
    virtual void destroy() override;

    int conn_establish(int fd, ucp_ep_h *ep,
                       uint64_t tag, EventCallbackRef conn_cb);

    void set_stack(UCXStack *s);
    UCXStack *get_stack() { return stack; }
};

class UCXConnectedSocketImpl : public ConnectedSocketImpl {
  private:
    uint64_t  dst_tag;
    ucp_ep_h  ucp_ep = NULL;

    UCXWorker *worker;

    int tcp_fd;
    int state; //Vasily: ??????

    std::deque<bufferlist*> pending;

    CephContext *cct() { return worker->cct; }

  public:
    UCXConnectedSocketImpl(UCXWorker *w);
    virtual ~UCXConnectedSocketImpl();

    int connect(const entity_addr_t& peer_addr, const SocketOptions &opt);
    int accept(int server_sock, entity_addr_t *out, const SocketOptions &opt);

    // interface functions
    virtual int is_connected() override;
    virtual ssize_t read(int, char*, size_t) override;
    virtual ssize_t zero_copy_read(bufferptr&) override;
    virtual ssize_t send(bufferlist &bl, bool more) override;
    virtual void shutdown() override;
    virtual void close() override;
    virtual int fd() const override { return tcp_fd; }

    //ucp request magic
    static void request_init(void *req);
    static void request_cleanup(void *req);

    static void send_completion_cb(void *request, ucs_status_t status);

    static void send_completion(ucx_req_descr *descr) {
        descr->bl->clear();
        if (descr->iov_list) {
            delete descr->iov_list;
        }
    }

    void handle_connection(int tag);

    class C_handle_connection : public EventCallback {
        UCXConnectedSocketImpl *conn;

        public:
            explicit C_handle_connection(UCXConnectedSocketImpl *c): conn(c) {}
            void do_request(int tag) override {
                conn->handle_connection(tag);
            }
    };
};

class UCXServerSocketImpl : public ServerSocketImpl {
  private:
    UCXWorker *worker;
    int server_setup_socket = -1;

    CephContext *cct() { return worker->cct; }
  public:
    UCXServerSocketImpl(UCXWorker *w);
    ~UCXServerSocketImpl();

    int listen(entity_addr_t &sa, const SocketOptions &opt);

    // interface functions
    virtual int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) override;
    virtual void abort_accept() override;
    // Get file descriptor
    virtual int fd() const override { return server_setup_socket; }
};

class UCXStack : public NetworkStack {
  vector<std::thread> threads;
  ucp_context_h ucp_context;
 public:
  explicit UCXStack(CephContext *cct, const string &t);
  virtual ~UCXStack();
  virtual bool support_zero_copy_read() const override { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return false; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;

  ucp_context_h get_ucp_context() { return ucp_context; }
};

#endif
