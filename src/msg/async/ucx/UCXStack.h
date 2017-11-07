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

extern "C" {
#include <ucp/api/ucp.h>
};

class UCXStack;
class UCXConnectedSocketImpl;

struct ucx_connect_message {
  uint64_t tag;
  uint16_t addr_len;
} __attribute__ ((packed));

struct ucx_rx_buf {
  size_t length;
  size_t offset;
  char data[0];
};


struct ucx_req_descr {
  class UCXConnectedSocketImpl *conn;
  bufferlist *bl;
  ucp_dt_iov_t *iov_list;
  ucx_rx_buf *rx_buf;
};

class DummyDataType {
public:
    DummyDataType() {
        ucs_status_t status = ucp_dt_create_generic(
                                        &dummy_datatype_ops,
                                        NULL, &ucp_datatype);
        if (status != UCS_OK) {
            //lderr(cct()) << __func__ << " ucp_dt_create_generic call failed." << dendl;
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

class UCXWorker : public Worker {
    ucp_worker_h ucp_worker;
    UCXStack *stack;
    ucp_address_t *ucp_addr;
    size_t ucp_addr_len;
    EventCallbackRef progress_cb;
    int ucp_fd = -1;

    std::list<UCXConnectedSocketImpl*> connections;

    class C_handle_worker_progress : public EventCallback {
        UCXWorker *worker;
    public:
        C_handle_worker_progress(UCXWorker *w): worker(w) {}
        void do_request(int fd) {
            worker->event_progress();
        }
    };

    DummyDataType dummy_dtype;

    void event_progress();
    // pass received messages to socket(s)
    void dispatch_rx();

public:
    explicit UCXWorker(CephContext *c, unsigned i);
    virtual ~UCXWorker();

    virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
    virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;

    virtual void initialize() override;
    virtual void destroy() override;

    void ucp_progress();
    void set_stack(UCXStack *s);

    UCXStack *get_stack() { return stack; }
    ucp_worker_h get_ucp_worker() { return ucp_worker; }

    // p2p ucp
    int send_addr(int sock, uint64_t tag);
    // recv address and create ep
    int recv_addr(int sock, ucp_ep_h *ep, uint64_t *tag);

    void drop_msgs(UCXConnectedSocketImpl *conn);
    void remove_conn(UCXConnectedSocketImpl *conn) {
        assert(center.in_thread());
        connections.remove(conn);
    }

    void add_conn(UCXConnectedSocketImpl *conn) {
        assert(center.in_thread());
        connections.push_back(conn);
    }

    int fd() {
        return ucp_fd;
    }
};

class UCXConnectedSocketImpl : public ConnectedSocketImpl {
  private:
    uint64_t  dst_tag;
    ucp_ep_h  ucp_ep;
    UCXWorker *worker;
    int tcp_fd;
    int state;

    std::deque<ucx_rx_buf *> rx_queue;

    CephContext *cct() { return worker->cct; }
    EventCallbackRef read_progress = nullptr;
    EventCallbackRef write_progress = nullptr;

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
    virtual void set_event_handlers(EventCallbackRef read_handler, EventCallbackRef write_handler) {
      read_progress = read_handler;
      write_progress = write_handler;
    }

    // receive dispatch

    //ucp request magic
    static void request_init(void *req);
    static void request_cleanup(void *req);

    static void send_completion_cb(void *request, ucs_status_t status);
    void send_completion(ucx_req_descr *descr) {
      descr->bl->clear();
      if (descr->iov_list)
        delete descr->iov_list;
    }
    // recv side
    static void recv_completion_cb(void *request, ucs_status_t status,
                            ucp_tag_recv_info_t *info);

    void dispatch_rx(ucx_rx_buf *buf);
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
