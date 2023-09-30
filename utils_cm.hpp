#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <thread>
#include <future>
#include <vector>
#include <iostream>
#include <string_view>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>


struct LOGGER {
    template<typename... Args>
    void operator()(Args&&... args) const {
        std::cout<<' '<<__FILE__<<' ';
        ((std::cout << args << ' '), ...);
        std::cout << std::endl; 
    }
}LOG;


class RDMAServer{
public:
    RDMAServer() = default;
    void listen(std::string_view ip, std::string_view port){
        chan_ = rdma_create_event_channel();
        if(!chan_)LOG(__LINE__, "failed to create event channel");

        rdma_create_id(chan_, &listen_id_, nullptr, RDMA_PS_TCP);
        if(!listen_id_)LOG(__LINE__, "failed to create listen cm_id");

        sockaddr_in sin{
        .sin_family = AF_INET,
        .sin_port = htons(atoi(port.data())),
        .sin_addr = {
            .s_addr = inet_addr(ip.data())
        }
        };
        if(rdma_bind_addr(listen_id_, (sockaddr*)&sin))LOG(__LINE__, "failed to bind addr");
        if(rdma_listen(listen_id_, 1))LOG(__LINE__, "failed to begin rdma listen");
        // pd_ = ibv_alloc_pd(listen_id_->verbs);
        int num_devices{};
        pd_ = ibv_alloc_pd(rdma_get_devices(&num_devices)[0]);
        if(!pd_)LOG(__LINE__, "failed to alloc pd");
        LOG(__LINE__, "ready to listen");

        rdma_cm_event *event{};
        for(;;){
            if(stop_)break;
            if(rdma_get_cm_event(chan_, &event))LOG(__LINE__, "failed to get cm event");
            switch (event->event)
            {
            case RDMA_CM_EVENT_CONNECT_REQUEST:{
                rdma_cm_id *cm_id{event->id};
                rdma_ack_cm_event(event);
                create_connection(cm_id);
                break;
            }
            case RDMA_CM_EVENT_DISCONNECTED:{
                for(auto&w:workers_)
                    if(w->cm_id_==event->id){
                        w->stop();
                        rdma_disconnect(w->cm_id_);
                        break;
                    }
                break;
            }
            default:
                rdma_ack_cm_event(event);
                break;
            }
        }
    }
    void stop(){
        stop_=true;
        for(auto&w:workers_)w->stop(), w->t_->join();
    }
    ~RDMAServer(){
        ibv_dealloc_pd(pd_);
        rdma_destroy_id(listen_id_);
        rdma_destroy_event_channel(chan_);
        for(auto&w:workers_)w->stop(), w->t_->join();
    }
private:
    class Worker{
    public:
        Worker()=default;
        void run(){
            ibv_wc wc;
            void *ctx{};
            for(;;){
                if(stop_)break;

                post_recv();
                if(ibv_get_cq_event(comp_chan_, &cq_, &ctx))LOG(__LINE__, "failed to get cq event");
                if(!ibv_poll_cq(cq_,1,&wc))LOG(__LINE__, "failed to poll cq");
                switch (wc.opcode)
                {
                case IBV_WC_RECV:{
                    if(!strcmp(resp_buf_,"BYE"))stop_=true;
                    // LOG(__LINE__, "server recv:", msg_buf_);
                    std::fill(resp_buf_, resp_buf_+sizeof(resp_buf_), 0);
                    strcpy(resp_buf_, msg_buf_);
                    std::reverse(resp_buf_, resp_buf_+strlen(resp_buf_));
                    post_send();
                    std::fill(msg_buf_, msg_buf_+sizeof(msg_buf_), 0);
                    break;
                }
                default:
                    break;
                }
                ibv_ack_cq_events(cq_, 1);
                if(ibv_req_notify_cq(cq_, 0))LOG(__LINE__, "failed to notify cq");
            }
        }
        void stop(){
            stop_=true;
            ibv_dereg_mr(resp_mr_);
            ibv_dereg_mr(msg_mr_);
            ibv_destroy_cq(cq_);
            ibv_destroy_comp_channel(comp_chan_);
        }
        ~Worker(){
            ibv_dereg_mr(resp_mr_);
            ibv_dereg_mr(msg_mr_);
            ibv_destroy_cq(cq_);
            ibv_destroy_comp_channel(comp_chan_);
        }
        rdma_cm_id *cm_id_{};
        ibv_mr *resp_mr_{}, *msg_mr_{};
        char resp_buf_[1024]{}, msg_buf_[1024]{};
        ibv_comp_channel *comp_chan_{};
        ibv_cq *cq_{};

        bool stop_{};
        std::thread *t_{};
    private:
        void post_recv(){
            ibv_sge sge{
                .addr = (uint64_t)msg_buf_,
                .length = sizeof(msg_buf_),
                .lkey = msg_mr_->lkey
            };
            ibv_recv_wr wr{
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1
            }, *bad_wr{};
            if(ibv_post_recv(cm_id_->qp, &wr, &bad_wr))LOG(__LINE__, "failed to post recv");
        }
        void post_send(){
            ibv_sge sge{
                .addr = (uint64_t)resp_buf_,
                .length = sizeof(resp_buf_),
                .lkey = resp_mr_->lkey
            };
            ibv_send_wr wr{
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = IBV_WR_SEND,
                .send_flags = IBV_SEND_SIGNALED
            }, *bad_wr;
            if(ibv_post_send(cm_id_->qp, &wr, &bad_wr))LOG(__LINE__, "failed to post send");
        }
    };
    void create_connection(rdma_cm_id* cm_id){
        int num_devices{};

        // ibv_comp_channel *comp_chan{ibv_create_comp_channel(listen_id_->verbs)};
        ibv_comp_channel *comp_chan{ibv_create_comp_channel(rdma_get_devices(&num_devices)[0])};
        if(!comp_chan)LOG(__LINE__, "failed to create ibv comp channel");

        // ibv_cq *cq{ibv_create_cq(listen_id_->verbs, 2, nullptr, comp_chan, 0)};
        ibv_cq *cq{ibv_create_cq(rdma_get_devices(&num_devices)[0], 2, nullptr, comp_chan, 0)};
        if(!cq)LOG(__LINE__, "failed to create cq");
        if(ibv_req_notify_cq(cq, 0))LOG(__LINE__, "failed to notify cq");

        ibv_qp_init_attr qp_init_attr{
                        .send_cq = cq,
                        .recv_cq = cq,
                        .cap{
                            .max_send_wr = 1,
                            .max_recv_wr = 1,
                            .max_send_sge = 1,
                            .max_recv_sge = 1
                        },
                        .qp_type = IBV_QPT_RC
                    };
        if(rdma_create_qp(cm_id, pd_, &qp_init_attr))LOG(__LINE__, "failed to create qp");

        Worker *worker = new Worker;
        worker->cm_id_ = cm_id, worker->cq_ = cq, worker->comp_chan_ = comp_chan;
        worker->msg_mr_ = ibv_reg_mr(pd_, worker->msg_buf_, sizeof(worker->msg_buf_), IBV_ACCESS_LOCAL_WRITE|
                                                                                      IBV_ACCESS_REMOTE_READ|
                                                                                      IBV_ACCESS_REMOTE_WRITE);
        worker->resp_mr_ = ibv_reg_mr(pd_, worker->resp_buf_, sizeof(worker->resp_buf_), IBV_ACCESS_LOCAL_WRITE|
                                                                                         IBV_ACCESS_REMOTE_READ|
                                                                                         IBV_ACCESS_REMOTE_WRITE);
        worker->t_ = new std::thread(&Worker::run, worker);
        workers_.emplace_back(worker);
        if(rdma_accept(cm_id, nullptr))LOG(__LINE__, "failed to accept connection");
    }
    bool stop_{};
    rdma_event_channel *chan_{};
    rdma_cm_id *listen_id_{};
    ibv_pd *pd_{};

    std::vector<Worker*> workers_;
};


class RDMAClient{
public:
    RDMAClient()=default;
    void connect(std::string_view ip, std::string_view port){
        chan_ = rdma_create_event_channel();
        if(!chan_)LOG(__LINE__, "failed to create rdma chan");

        if(rdma_create_id(chan_, &cm_id_, nullptr, RDMA_PS_TCP))LOG(__LINE__, "failed to create cmid");

        if (!cm_id_) {
            abort();
        }
        
        addrinfo *res;
        if(getaddrinfo(ip.data(), port.data(), nullptr, &res)<0)LOG(__LINE__, "failed to resolve addr");

        addrinfo *t{res};
        for(;t;t=t->ai_next)
            if(!rdma_resolve_addr(cm_id_, nullptr, t->ai_addr, 500))break;
        if(!t)LOG(__LINE__, "failed to resolve addr");

        rdma_cm_event *event{};
        if(rdma_get_cm_event(chan_, &event))LOG(__LINE__, "failed to get cm event");
        if(event->event != RDMA_CM_EVENT_ADDR_RESOLVED)LOG(__LINE__, "failed to resolve addr info");
        rdma_ack_cm_event(event);

        if(rdma_resolve_route(cm_id_, 1000))LOG(__LINE__, "failed to resolve route");
        if(rdma_get_cm_event(chan_, &event))LOG(__LINE__, "failed to get cm event");
        if(event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)LOG(__LINE__, "failed to resolve route");
        rdma_ack_cm_event(event);
        
        pd_ = ibv_alloc_pd(cm_id_->verbs);
        if(!pd_)LOG(__LINE__, "failed to alloc pd");

        comp_chan_ = ibv_create_comp_channel(cm_id_->verbs);
        if(!comp_chan_)LOG(__LINE__, "failed to create comp chan");

        cq_ = ibv_create_cq(cm_id_->verbs, 2, nullptr, comp_chan_, 0);
        if(!cq_)LOG(__LINE__, "failed to create cq");

        if(ibv_req_notify_cq(cq_, 0))LOG(__LINE__, "failed to notify cq");
        
        ibv_qp_init_attr qp_init_attr{
                        .send_cq = cq_,
                        .recv_cq = cq_,
                        .cap{
                            .max_send_wr = 1,
                            .max_recv_wr = 1,
                            .max_send_sge = 1,
                            .max_recv_sge = 1
                        },
                        .qp_type = IBV_QPT_RC
                    };
        if(rdma_create_qp(cm_id_, pd_, &qp_init_attr))LOG(__LINE__, "failed to create qp");
        if(rdma_connect(cm_id_, nullptr))LOG(__LINE__, "failed to connect");
        if(rdma_get_cm_event(chan_, &event))LOG(__LINE__, "failed to get cm event");

        if(event->event != RDMA_CM_EVENT_ESTABLISHED)LOG(__LINE__, "failed to establish connect");
        msg_mr_ = ibv_reg_mr(pd_, msg_buf_, sizeof(msg_buf_), IBV_ACCESS_LOCAL_WRITE|
                                                              IBV_ACCESS_REMOTE_READ|
                                                              IBV_ACCESS_REMOTE_WRITE);
        resp_mr_ = ibv_reg_mr(pd_, resp_buf_, sizeof(resp_buf_), IBV_ACCESS_LOCAL_WRITE|
                                                                 IBV_ACCESS_REMOTE_READ|
                                                                 IBV_ACCESS_REMOTE_WRITE);
    }
    void post_send(std::string_view msg){
        memset(msg_buf_, 0, sizeof(msg_buf_));
        strcpy(msg_buf_, msg.data());
        ibv_sge sge{
                .addr = (uint64_t)msg_buf_,
                .length = sizeof(msg_buf_),
                .lkey = msg_mr_->lkey
            };
            ibv_send_wr wr{
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = IBV_WR_SEND,
                .send_flags = IBV_SEND_SIGNALED
            }, *bad_wr;

        void *ctx{};
        ibv_wc wc{};
        if(ibv_post_send(cm_id_->qp, &wr, &bad_wr))LOG(__LINE__, "failed to post send");
        if(ibv_get_cq_event(comp_chan_, &cq_, &ctx))LOG(__LINE__, "failed to get cq event");
        if(!ibv_poll_cq(cq_,1,&wc))LOG(__LINE__, "failed to poll cq");
        if(ibv_req_notify_cq(cq_, 0))LOG(__LINE__, "failed to notify cq");
        ibv_ack_cq_events(cq_, 1);
    }
    std::string post_recv(){
        memset(resp_buf_, 0, sizeof(resp_buf_));
        ibv_sge sge{
                .addr = (uint64_t)resp_buf_,
                .length = sizeof(resp_buf_),
                .lkey = resp_mr_->lkey
            };
            ibv_recv_wr wr{
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1
            }, *bad_wr{};

        void *ctx{};
        ibv_wc wc{};        
        if(ibv_post_recv(cm_id_->qp, &wr, &bad_wr))LOG(__LINE__, "failed to post recv");
        if(ibv_get_cq_event(comp_chan_, &cq_, &ctx))LOG(__LINE__, "failed to get cq event");
        if(!ibv_poll_cq(cq_, 1, &wc))LOG(__LINE__, "failed to poll cq");
        if(ibv_req_notify_cq(cq_, 0))LOG(__LINE__, "failed to notify cq");
        ibv_ack_cq_events(cq_, 1);
        return resp_buf_;
    }
    void close(){
        post_send("BYE");
        rdma_disconnect(cm_id_);

        rdma_cm_event *event{};
        if(rdma_get_cm_event(chan_, &event))LOG(__LINE__, "failed to get cm event");
        if(event->event != RDMA_CM_EVENT_DISCONNECTED)LOG(__LINE__, "failed to disconnect");
        LOG(__LINE__, "client closed");
    }
    ~RDMAClient(){
        ibv_dealloc_pd(pd_);
        ibv_dereg_mr(msg_mr_), ibv_dereg_mr(resp_mr_);
        ibv_destroy_cq(cq_);
        ibv_destroy_comp_channel(comp_chan_);
        rdma_destroy_id(cm_id_);
        rdma_destroy_event_channel(chan_);
    }
private:
    rdma_event_channel *chan_{};
    rdma_cm_id *cm_id_{};
    ibv_pd *pd_{};
    ibv_comp_channel *comp_chan_{};
    ibv_cq *cq_{};
    ibv_mr *msg_mr_{}, *resp_mr_{};
    char msg_buf_[1024]{}, resp_buf_[1024]{};
};