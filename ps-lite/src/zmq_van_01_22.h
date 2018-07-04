/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif

/********used to record the trace*************/
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>
#include "ps/internal/postoffice.h"
#include <netdb.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>
#include <assert.h>
#include <thread>
/*********** the end **********************/

namespace ps {
/**
 * \brief be smart on freeing recved data
 */

inline void FreeData(void *data, void *hint) {
  if (hint == NULL) {
    delete [] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/**
 * \brief ZMQ based implementation
 */
class ZMQVan : public Van {
 public:
  ZMQVan() { }
  virtual ~ZMQVan() { }

 protected:
  void Start() override {
    // start zmq
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
    // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);
    Van::Start();
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    zmq_ctx_destroy(context_);
  }

  int Bind(const Node& node, int max_retry) override {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL)+port);
    for (int i = 0; i < max_retry+1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    return port;
  }

  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) &&
        (node.id != my_node_.id)) {
      return;
    }
    void *sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
    }
    // connect
    std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) <<  "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    senders_[id] = sender;
  }

 
/*********************************** xym edit ***********************************************/

    void Trace_Send(const Message& msg, int bytes) override {
      std::lock_guard<std::mutex> lk(xu_);//避免不同线程的同时写
      gettimeofday(&end, NULL);
      int send_id;
      int recv_id;
      int dep_time;
      int num_workers;
      int num_servers;
      std::string operation;
      if(msg.meta.push) operation = "push_operation";
      if(!msg.meta.push) operation = "pull_operation";
//	if(msg.meta.push == NULL) operation = "Send_NULL_Value";
      char host[100] ={0};//used to record the hostname of node
      gethostname(host,sizeof(host));
      send_id = Postoffice::Get()->send_id_();//如果是scheduler节点，需要单独考虑，scheduler的不能这样算，scheduler.id = 1
      num_workers = Postoffice::Get()->num_workers();
      num_servers = Postoffice::Get()->num_servers();
      if (Postoffice::Get()->is_worker()) {
        if(msg.meta.recver == 1){
          recv_id = msg.meta.recver + num_servers + num_workers - 1;//接收的节点为scheduler节点
        }
        else
          recv_id = (msg.meta.recver - 8) / 2 + num_workers;//编号最开始是worker，之后是server,最后是scheduler
        if (record_count == 0) {
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
          //    fprintf(file, "%d\tworker%d\tserver%d\t%d\t%d\t%d\t%d\n", record_count, send_id, recv_id, bytes,
          //      0, 0, record_count - 1);
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\two_s_f_msg.meta.recver:=%d\tmsg.meta.sender:=%d\tnum_workers:=%d\tnum_servers:=%d\trecv_id:=%d\tworker_hostname:=\t%s\n",
                  record_count, send_id, recv_id, bytes, 0, 0, record_count - 1,msg.meta.push_pull_operation, operation.c_str(), msg.meta.recver, msg.meta.sender,num_workers, num_servers,recv_id,host);
          fclose(file);
        }
        else {//如果不是第一次send，则存在接收依赖；
          dep_time = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\two_s_p_msg.meta.recver:=%d\tthread_lwid:=%u\tthread_tid:=%u\n", record_count, send_id, recv_id, bytes,
                  2, dep_time, record_count - 1,msg.meta.push_pull_operation, operation.c_str(), msg.meta.recver,syscall(SYS_gettid),pthread_self());
          fclose(file);
        }
      }

      if (Postoffice::Get()->is_server()) {
        if(msg.meta.recver == 1){
          recv_id = msg.meta.recver + num_servers + num_workers - 1;
        }
        else
          recv_id = (msg.meta.recver - 9) / 2;
        send_id = send_id + num_workers;
        if (record_count == 0) {
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
//                fprintf(file, "%d\tserver%d\tworker%d\t%d\t%d\t%d\t%d\n", record_count, send_id, recv_id, bytes,
//                  0, 0, record_count - 1);
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\tser_s_f_msg.meta.recver:=%d\tnum_workers:=%d\tnum_servers:=%d\trecv_id:=%d\tserver_hostname:=\t%s\n",
                  record_count, send_id, recv_id, bytes, 0, 0, record_count - 1, msg.meta.push_pull_operation, operation.c_str(), msg.meta.recver,num_workers,num_servers,recv_id,host);
          fclose(file);
        }
        else {//如果不是第一次send，则存在接收依赖；
          dep_time = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\tser_s_p_msg.meta.recver:=%d\tthread_lwid:=%u\tthread_tid:=%u\n", record_count, send_id, recv_id, bytes,
                  2, dep_time, record_count - 1, msg.meta.push_pull_operation, operation.c_str(), msg.meta.recver,syscall(SYS_gettid),pthread_self());
          fclose(file);
        }
      }
      if(Postoffice::Get()->is_scheduler()){
//            printf("Check the push_pull_operation:== %d\n", msg.meta.push_pull_operation);
        send_id = num_servers + num_workers;//对于接收节点是worker还是server需要分开考虑
        if(msg.meta.recver == 1)
          recv_id = num_servers + num_workers;
        else{
          if(msg.meta.recver%2 == 0)
            recv_id = (msg.meta.recver - 8)/2 + num_workers;
          else
            recv_id = (msg.meta.recver - 9)/2;
        }
        if(record_count == 0) {
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\tsch_s_f_msg.meta.recver:=%d\tscheduler_hostname:=\t%s\n", record_count,
                  send_id, recv_id, bytes, 0, 0, record_count - 1, msg.meta.push_pull_operation, operation.c_str(), msg.meta.recver, host);
          fclose(file);
        }
        else{
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\tsch_s_p_msg.meta.recver:=%d\n", record_count,
                  send_id, recv_id, bytes, 0, 0, record_count - 1,msg.meta.push_pull_operation, operation.c_str(),  msg.meta.recver);
          fclose(file);
        }
      }
      record_count++;
    }

    void Trace_Recv(Message* msg, size_t bytes) override {
//      std::lock_guard<std::mutex> lk(xu_);//避免不同线程的同时写
      gettimeofday(&start,NULL);
      int send_id;//记录发送的节点
      int recv_id;//记录接收的节点
      int dep_time;
      int num_workers;
      int num_servers;
      std::string operation;
      if (msg->meta.push == true) operation="push_operation";
      if (msg->meta.push == false) operation = "pull_operation";
//      if (msg->meta.push == NULL) operation = "Recv_NULL_Value";
      char host[100] = {0};
      gethostname(host,sizeof(host));
      num_workers = Postoffice::Get()->num_workers();
      num_servers = Postoffice::Get()->num_servers();
//      recv_id = Postoffice::Get()->send_id_();//返回的时机上是本地节点的rank，调用了my_rank函数
      recv_id = msg->meta.recver;
      if(Postoffice::Get()->is_worker()){
        recv_id = (recv_id - 9)/2;
        if(msg->meta.sender%2==0)
          send_id = (msg->meta.sender - 8)/2 + num_workers;//此处发送的节点是server节点；
        else
          send_id = msg->meta.sender + num_servers + num_workers - 1;//发送的节点是scheduler节点；
        if(record_count == 0){
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_w_f_msg.meta.recver:=%d\tworker_hostname:=\t%s\n", record_count,
                  send_id, recv_id, bytes, 0, 0, record_count - 1,msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver,host);
          fclose(file);
        }
        else{
          dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_w_plus_msg.meta.recver:=%d\tthread_lwid:=%u\tthread_tid:=%u\n", record_count,
                  send_id, recv_id, bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver,syscall(SYS_gettid),pthread_self());
          fclose(file);
        }
      }
      if(Postoffice::Get()->is_server()){
        recv_id = (recv_id - 8)/2;
        recv_id = recv_id + num_workers;
        if(msg->meta.sender == 1)
          send_id = msg->meta.sender + num_servers + num_workers - 1;
        else
          send_id = (msg->meta.sender - 9)/2;
        if(record_count == 0){
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_ser_f_msg.meta.recver:=%d\tserver_hostname:=\t%s\n", record_count,
                  send_id, recv_id, bytes, 0, 0, record_count - 1, msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver, host);
          fclose(file);
        }
        else{
          dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_ser_plus_msg.meta.recver:=%d\tthread_lwid:=%u\tthread_tid:=%u\n", record_count,
                  send_id, recv_id, bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver, syscall(SYS_gettid),pthread_self());
          fclose(file);
        }
      }
      if(Postoffice::Get()->is_scheduler()){
        recv_id = num_servers + num_workers;//此处的接收节点是scheduler节点，
        dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
        if(msg->meta.sender == 1)
          send_id = num_workers + num_servers;
        else{
          if(msg->meta.sender%2 == 0){
            send_id = (msg->meta.sender - 8)/2 + num_workers;
          }
          else
            send_id = (msg->meta.sender - 9)/2;
        }
        if(scheduler_count<=num_workers + num_servers){
          send_id = scheduler_count;
          scheduler_count++;
          dep_time = 0;
        }
        if(record_count == 0){
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_sch_f_msg.meta.recver:=%d\tscheduler_hostname:=%s\n", record_count,
                  send_id, recv_id, bytes, 0, 0, record_count - 1, msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver,host);
          fclose(file);
        }
        else{
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
          fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= %s\t\trec_sch_plus_msg.meta.recver:=%d\n", record_count,
                  send_id, recv_id, bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, operation.c_str(), msg->meta.recver);
          fclose(file);
        }
      }
      record_count++;
    }

    void foo()
    {
      FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt","a");
      for(int i = 0; i< 500; i++)
      {
        fprintf(file,"record_count:=%d\t\t%d\n", i, trace_[i]);
      }
      fclose(file);
    }


//=====================================================================================================================//
//                                                                                                                     //
//                                  This is used to distinguish scheduler from worker and server                       //
//                                                                                                                     //
//******************************************** xym edit 2017-12-25 ****************************************************//
    void Trace_Send_(const Message& msg, int send_bytes, int key) override {
        std::lock_guard<std::mutex> lk(xu_);//避免不同线程的同时写
        gettimeofday(&end, NULL);
        int send_id;
        int recv_id;
        int dep_time;
        int num_workers;
        int num_servers;
        std::string operation;
        if(msg.meta.push) operation = "push_operation";
        if(!msg.meta.push) operation = "pull_operation";
        //    if(msg.meta.push == NULL) operation = "Send_NULL_Value";
        char host[100] ={0};//used to record the hostname of node
        gethostname(host,sizeof(host));
        send_id = Postoffice::Get()->send_id_();//如果是scheduler节点，需要单独考虑，scheduler的不能这样算，scheduler.id = 1
        num_workers = Postoffice::Get()->num_workers();
        num_servers = Postoffice::Get()->num_servers();
        
        if(Postoffice::Get()->is_scheduler()){
            //            printf("Check the push_pull_operation:== %d\n", msg.meta.push_pull_operation);
            send_id = num_servers + num_workers;//对于接收节点是worker还是server需要分开考虑
            if(msg.meta.recver == 1)
                recv_id = num_servers + num_workers;
            else{
                if(msg.meta.recver%2 == 0)
                    recv_id = (msg.meta.recver - 8)/2 + num_workers;
                else
                    recv_id = (msg.meta.recver - 9)/2;
            }
            if(record_sch == 0) {
                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt", "a");
                fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
                fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_Comm\t\tsch_s_f_msg.meta.recver:=%d\tscheduler_hostname:=\t%s\n", record_sch,
                        send_id, recv_id, send_bytes, 0, 0, record_sch - 1, msg.meta.push_pull_operation,msg.meta.recver, host);
                fclose(file);
            }
            else{
                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_Comm\t\tsch_s_p_msg.meta.recver:=%d\n", record_sch,
                        send_id, recv_id, send_bytes, 0, 0, record_sch - 1,msg.meta.push_pull_operation,  msg.meta.recver);
                fclose(file);
            }
            record_sch++;
        }
        
//============================================ This is the worker operation =============================================//
        if (Postoffice::Get()->is_worker()) {
            if(msg.meta.recver == 1){
                recv_id = msg.meta.recver + num_servers + num_workers - 1;//接收的节点为scheduler节点
                if (record_sch == 0) {
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt", "a");
                    fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
                    //    fprintf(file, "%d\tworker%d\tserver%d\t%d\t%d\t%d\t%d\n", record_count, send_id, recv_id, bytes,
                    //      0, 0, record_count - 1);
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Worker_Send_Sch\t\two_s_f_msg.meta.recver:=%d\tmsg.meta.sender:=%d\tnum_workers:=%d\tnum_servers:=%d\trecv_id:=%d\tworker_hostname:=\t%s\n",
                            record_sch, send_id, recv_id, send_bytes, 0, 0, record_sch - 1, msg.meta.push_pull_operation, msg.meta.recver, msg.meta.sender,num_workers, num_servers,recv_id,host);
                    fclose(file);
                }
                else{//如果不是第一次send，则存在接收依赖；
                    dep_time = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Worker_Send_Sch\t\two_s_p_msg.meta.recver:=%d\n", record_sch, send_id, recv_id, send_bytes,
                            2, dep_time, record_sch - 1,msg.meta.push_pull_operation, msg.meta.recver);
                    fclose(file);
                }
                record_sch++;
            }//end receiver == scheduler;
            else{
                recv_id = (msg.meta.recver - 8) / 2 + num_workers;//编号最开始是worker，之后是server，最后是scheduler；此处接收节点为server节点
                if(msg.meta.push){
                    //此处为Push_Send
                    if(Init_Flag_Wor[key] == 0){
                        Trace_Push_Send[key].operation_count = 0;
                        Trace_Push_Send[key].time            = end;
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
			    fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation,key, Trace_Push_Send[key].operation_count, 0, 0, end.tv_sec, end.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Trace_Push_Send[key].operation_count, 0, 0,  end.tv_sec, end.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                        Init_Flag_Wor[key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                    }//end Init_Flag == 0;
                    else{
                        Trace_Push_Send[key].operation_count = Trace_Pull_Recv[key].operation_count+1;
                        Trace_Push_Send[key].time            = end;
                        dep_time = 1000000 * (end.tv_sec - (Trace_Pull_Recv[key].time).tv_sec) + end.tv_usec - (Trace_Pull_Recv[key].time).tv_usec;
                        FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                        fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Trace_Push_Send[key].operation_count, 4, dep_time, end.tv_sec, end.tv_usec, Trace_Pull_Recv[key].operation_count);//push_Send依赖于所有key值的Recv_Pull操作的完成；
                        fclose(file);
                    }
                }//end msg.meta.push
                else
                {
                    //此为SendCommandToServers, key为-1, msg.meta.push_pull_operation的值为0
                    if(msg.meta.recv_key == -1){
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
			    fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");                            
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_TO_Servers\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_To_Servers\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                    }//end SendCommandsToServers
                    else{
                        //此处为Pull_Send
                        if(Init_Flag_Wor[key] == 0){
                            Trace_Pull_Send[key].operation_count = 0;
                            Trace_Pull_Send[key].time            = end;
                            if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file,"====================================================================================================================================\n");
                            	fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    	fprintf(file,"==		 Worker_ID: = ");
			    	for(int w_num = 0; w_num < num_workers; w_num++){
			    		fprintf(file,"%d,",w_num);	
			    	}
			    	fprintf(file,"\tServer_ID: =");
			    	for(int s_num = 0; s_num < num_servers; s_num++)
					fprintf(file,"%d,", s_num);
			    	fprintf(file,"\n");
			    	fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    	fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    	fprintf(file,"==     push =|\n");
			    	fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    	fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    	fprintf(file,"==     pull =|\n");
			    	fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    	fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    	fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    	fprintf(file,"=====================================================================================================================================\n"); 
                                fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Trace_Pull_Send[key].operation_count, 0, 0, end.tv_sec, end.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                                fclose(file);
                            }
                            else{
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Trace_Pull_Send[key].operation_count, 0, 0, end.tv_sec, end.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                                fclose(file);
                            }
                            Init_Flag_Wor[key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                        }//end Init_Flag == 0;
                        else{
                            dep_time = 1000000 * (end.tv_sec - (Trace_Push_Recv[key].time).tv_sec) + end.tv_usec - (Trace_Push_Recv[key].time).tv_usec;
                            Trace_Pull_Send[key].operation_count = Trace_Push_Recv[key].operation_count+1;
                            Trace_Pull_Send[key].time            = end;
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Trace_Pull_Send[key].operation_count, 2, dep_time, end.tv_sec, end.tv_usec, key, Trace_Push_Recv[key].operation_count);//push_Send依赖于所有key值的Recv_Pull操作的完成；
                            fclose(file);
                        }
                    }//end Pull_Send
                }
                record_count++;
            }
        }
//========================================= This is about the Server Operation 2017-12-25 ==========================================//
        if (Postoffice::Get()->is_server()) {
            if(msg.meta.recver == 1){
                recv_id = msg.meta.recver + num_servers + num_workers - 1;//接收的节点为scheduler节点
                if (record_sch == 0) {
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt", "a");
                    fprintf(file, "ID\tsrc\tdst\tlength\t\tDep\t\tdTime\t\tIDdep\t\tDep_PP\n");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Server_Send_Sch\t\two_s_f_msg.meta.recver:=%d\tmsg.meta.sender:=%d\tnum_workers:=%d\tnum_servers:=%d\trecv_id:=%d\tworker_hostname:=\t%s\n",
                            record_count, send_id, recv_id, send_bytes, 0, 0, record_count - 1,msg.meta.push_pull_operation, msg.meta.recver, msg.meta.sender,num_workers, num_servers,recv_id,host);
                    fclose(file);
                }
                else{//如果不是第一次send，则存在接收依赖；
                    dep_time = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Server_Send_Sch\t\two_s_p_msg.meta.recver:=%d\n", record_count, send_id, recv_id, send_bytes,
                            2, dep_time, record_count - 1,msg.meta.push_pull_operation, msg.meta.recver);
                    fclose(file);
                }
                record_sch++;
            }//msg.meta.recver==scheduler
            else{
                recv_id = (msg.meta.recver - 9) / 2;//接收的节点为worker节点；
                send_id = send_id + num_workers;
                if(msg.meta.push){
                    if(Init_Flag_Ser[recv_id][key] == 0){
                        Ser_Push_Send[recv_id][key].operation_count = 0;
                        Ser_Push_Send[recv_id][key].time            = end;
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     server_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Push_Send[recv_id][key].operation_count, recv_id, 0, 0, end.tv_sec, end.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Push_Send[recv_id][key].operation_count, recv_id, 0, 0, end.tv_sec, end.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                        Init_Flag_Ser[recv_id][key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                    }//end Init_Flag == 0;
                    else{
                        dep_time = 1000000 * (end.tv_sec - (Ser_Push_Recv[recv_id][key].time).tv_sec) + end.tv_usec - (Ser_Push_Recv[recv_id][key].time).tv_usec;
                        Ser_Push_Send[recv_id][key].operation_count = Ser_Push_Recv[recv_id][key].operation_count+1;
                        Ser_Push_Send[recv_id][key].time            = end;
                        FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                        fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t",
                                record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Push_Send[recv_id][key].operation_count, recv_id, 1, dep_time, end.tv_sec, end.tv_usec);//push_Send依赖于所有key值的Recv_Pull操作的完成；
                        //============同步模式下的依赖关系===========//
                        if(msg.meta.push_pull_operation == -15){
			    fprintf(file,"%d-%d-w%d\n", key, Ser_Push_Recv[recv_id][key].operation_count,recv_id);
			    fclose(file);
			}
			else{
			    fprintf(file,"(");
                            for(int i = 0; i < num_workers; ++i)
                                fprintf(file,"%d-%d-w%d, ", key, Ser_Push_Recv[i][key].operation_count, i);
                        	fprintf(file, ")\n");
                        	fclose(file);
                        //============异步模式下的依赖关系===========//
                        /*fprintf(file,"w%d_%d-%d\n",recv_id, Ser_Push_Recv[recv_id][key].operation_count, key);
                         fclose(file);*/
			}
                    }
                }//meta.Push_send
                else{
                    //此为Pull_Send
                    if(msg.meta.recv_key == -1){
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     server_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_TO_Workers\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_To_Workers\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                    }//end SendCommandsToServers
                    else{
                        if(Init_Flag_Ser[recv_id][key] == 0){
                            Ser_Pull_Send[recv_id][key].operation_count = 0;
                            Ser_Pull_Send[recv_id][key].time            = end;
                            if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file,"====================================================================================================================================\n");
                            	fprintf(file,"==		 num_workers: = %d     num_servers: = %d     server_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    	fprintf(file,"==		 Worker_ID: = ");
			    	for(int w_num = 0; w_num < num_workers; w_num++){
			    		fprintf(file,"%d,",w_num);	
			    	}
			    	fprintf(file,"\tServer_ID: =");
			    	for(int s_num = 0; s_num < num_servers; s_num++)
					fprintf(file,"%d,", s_num);
			    	fprintf(file,"\n");
			    	fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    	fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n ");
			    	fprintf(file,"==     push =|\n");
			    	fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n ");
			    	fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n ");
			    	fprintf(file,"==     pull =|\n");
			    	fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n ");
			    	fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    	fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    	fprintf(file,"=====================================================================================================================================\n");
                                fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Pull_Send[recv_id][key].operation_count, recv_id, 0, 0, end.tv_sec, end.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                                fclose(file);
                            }
                            else{
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Push_Send[recv_id][key].operation_count, recv_id, 0, 0, end.tv_sec, end.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                                fclose(file);
                            }
                            Init_Flag_Ser[recv_id][key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                        }//end Init_Flag == 0;
                        else{
                            dep_time = 1000000 * (end.tv_sec - (Ser_Pull_Recv[recv_id][key].time).tv_sec) + end.tv_usec - (Ser_Pull_Recv[recv_id][key].time).tv_usec;
                            Ser_Pull_Send[recv_id][key].operation_count = Ser_Pull_Recv[recv_id][key].operation_count+1;
                            Ser_Pull_Send[recv_id][key].time            = end;
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Send_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d-w%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg.meta.push_pull_operation, key, Ser_Pull_Send[recv_id][key].operation_count, recv_id, 3, dep_time, end.tv_sec, end.tv_usec, key, Ser_Pull_Recv[recv_id][key].operation_count, recv_id);//push_Send依赖于所有key值的Recv_Pull操作的完成；
                            fclose(file);
                        }
                    }
                }//meta.pull_send
                record_count++;
            }//else: msg.meta.recver = worker
        }
    }
    
    void Trace_Recv_(Message* msg, size_t send_bytes, int key) override {
        //      std::lock_guard<std::mutex> lk(xu_);//避免不同线程的同时写
        gettimeofday(&start,NULL);
        int send_id;//记录发送的节点
        int recv_id;//记录接收的节点
        int dep_time;
        int num_workers;
        int num_servers;
        std::string operation;
        if (msg->meta.push == true) operation="push_operation";
        if (msg->meta.push == false) operation = "pull_operation";
        //      if (msg->meta.push == NULL) operation = "Recv_NULL_Value";
        char host[100] = {0};
        gethostname(host,sizeof(host));
        num_workers = Postoffice::Get()->num_workers();
        num_servers = Postoffice::Get()->num_servers();
        //      recv_id = Postoffice::Get()->send_id_();//返回的时机上是本地节点的rank，调用了my_rank函数
        recv_id = msg->meta.recver;
        
//========================================== this is about the scheduler operation =======================================//
        if(Postoffice::Get()->is_scheduler()){
            recv_id = num_servers + num_workers;//此处的接收节点是scheduler节点，
            dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
            if(msg->meta.sender == 1)
                send_id = num_workers + num_servers;
            else{
                if(msg->meta.sender%2 == 0){
                    send_id = (msg->meta.sender - 8)/2 + num_workers;
                }
                else
                    send_id = (msg->meta.sender - 9)/2;
            }
            /*
            if(scheduler_count<=num_workers + num_servers){//该操作的作用已经记不住了
                send_id = scheduler_count;
                scheduler_count++;
                dep_time = 0;
            }*/
            if(record_sch == 0){
                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
                fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_Comm\t\trec_sch_f_msg.meta.recver:=%d\tscheduler_hostname:=%s\n", record_count,
                        send_id, recv_id, send_bytes, 0, 0, record_count - 1, msg->meta.push_pull_operation, msg->meta.recver,host);
                fclose(file);
            }
            else{
                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_Comm\t\trec_sch_plus_msg.meta.recver:=%d\n", record_count,
                        send_id, recv_id, send_bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, msg->meta.recver);
                fclose(file);
            }
            record_sch++;
        }
//======================================= this is about the worker operation =========================================//
        if(Postoffice::Get()->is_worker()){
            recv_id = (recv_id - 9)/2;
            if(msg->meta.sender%2 != 0){
                send_id = msg->meta.sender + num_servers + num_workers - 1;//发送的节点是scheduler节点；
                if(record_sch == 0){
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "ID\tsrc\tdst\tlength\tDep\tdTime\tIDdep\tDep_PP\n");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_to_Worker\t\trec_w_f_msg.meta.recver:=%d\tworker_hostname:=\t%s\n", record_count,
                            send_id, recv_id, send_bytes, 0, 0, record_count - 1,msg->meta.push_pull_operation, msg->meta.recver,host);
                    fclose(file);
                }
                else{
                    dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_to_Worker\t\trec_w_plus_msg.meta.recver:=%d\n", record_count,
                            send_id, recv_id, send_bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, msg->meta.recver);
                    fclose(file);
                }
                record_sch++;
            }//msg.sender == scheduler;
            else{
                send_id = (msg->meta.sender - 8)/2 + num_workers;//此处发送的节点是server节点；
                if(msg->meta.push){
                    if(Init_Flag_Wor[key] == 0){
                        Trace_Push_Recv[key].operation_count = 0;
                        Trace_Push_Recv[key].time            = end;
                        if(record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Push_Recv[key].operation_count, 0, 0, start.tv_sec, start.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Push_Recv[key].operation_count, 0, 0, start.tv_sec, start.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                        Init_Flag_Wor[key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                    }//end Init_Flag == 0;
                    else{
                        dep_time = 1000000 * (start.tv_sec - (Trace_Push_Send[key].time).tv_sec) + start.tv_usec - (Trace_Push_Send[key].time).tv_usec;
                        Trace_Push_Recv[key].operation_count = Trace_Push_Send[key].operation_count+1;
                        Trace_Push_Recv[key].time            = start;
                        FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                        fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d\n",
                                record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Push_Recv[key].operation_count, 1, dep_time, start.tv_sec, start.tv_usec, key, Trace_Push_Send[key].operation_count);//push_Recv依赖于push_send操作的完成；依赖关系为push-on-push
                        fclose(file);
                    }
                }//end Push_Recv
                else{
                    //此为Pull_Recv
                    if(msg->meta.recv_key == -1){
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_TO_Servers\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_To_Servers\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation);//无需体现其依赖性
                            fclose(file);
                        }
                    }//end SendCommandsToServers
                    else{
                        if(Init_Flag_Wor[key] == 0){
                            Trace_Pull_Recv[key].operation_count = 0;
                            Trace_Pull_Recv[key].time            = start;
                            if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file,"====================================================================================================================================\n");
                            	fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    	fprintf(file,"==		 Worker_ID: = ");
			    	for(int w_num = 0; w_num < num_workers; w_num++){
			    		fprintf(file,"%d,",w_num);	
			    	}
			    	fprintf(file,"\tServer_ID: =");
			    	for(int s_num = 0; s_num < num_servers; s_num++)
					fprintf(file,"%d,", s_num);
			    	fprintf(file,"\n");
			    	fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    	fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n ");
			    	fprintf(file,"==     push =|\n");
			   	fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n ");
			    	fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n ");
			    	fprintf(file,"==     pull =|\n");
			    	fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n ");
			    	fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    	fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    	fprintf(file,"=====================================================================================================================================\n");
                                fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Pull_Recv[key].operation_count,0, 0, start.tv_sec, start.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                                fclose(file);
                            }
                            else{
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Pull_Recv[key].operation_count, 0, 0, start.tv_sec, start.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                                fclose(file);
                            }
                            Init_Flag_Wor[key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                        }//end Init_Flag == 0;
                        else{
                            dep_time = 1000000 * (start.tv_sec - (Trace_Pull_Send[key].time).tv_sec) + start.tv_usec - (Trace_Pull_Send[key].time).tv_usec;
                            Trace_Pull_Recv[key].operation_count = Trace_Pull_Send[key].operation_count+1;
                            Trace_Pull_Recv[key].time            = end;
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Worker\t\t%d-%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Trace_Pull_Recv[key].operation_count, 3, dep_time, start.tv_sec, start.tv_usec, key, Trace_Pull_Send[key].operation_count);//push_Recv依赖于push_send操作的完成；依赖关系为push-on-push
                            fclose(file);
                        }
                    }
                }//end Pull_Recv
                record_count++;
            }//msg.sender == server;
        }
//======================================= this is about the server operation ========================================//
        if(Postoffice::Get()->is_server()) {
            recv_id = (recv_id - 8)/2;
            recv_id = recv_id + num_workers;
            if(msg->meta.sender == 1){
                send_id = msg->meta.sender + num_servers + num_workers - 1;//发送的节点是scheduler节点；
                if(record_sch == 0){
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "ID\tsrc\tdst\tlength\\ttDep\t\tdTime\t\tIDdep\t\tDep_PP\n");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_to_Server\t\trec_w_f_msg.meta.recver:=%d\tworker_hostname:=\t%s\n", record_count,
                            send_id, recv_id, send_bytes, 0, 0, record_count - 1, msg->meta.push_pull_operation, msg->meta.recver,host);
                    fclose(file);
                }
                else{
                    dep_time = 1000000*(start.tv_sec - end.tv_sec) + (start.tv_usec - end.tv_usec);
                    FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_sch_record.txt","a");
                    fprintf(file, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\tOP:= Scheduler_to_Server\t\trec_w_plus_msg.meta.recver:=%d\n", record_count,
                            send_id, recv_id, send_bytes, 1, dep_time, record_count - 1, msg->meta.push_pull_operation, msg->meta.recver);
                    fclose(file);
                }
                record_sch++;
            }//end sender == scheduler
            else{
                send_id = (msg->meta.sender - 9)/2;
                if(msg->meta.push) {
                    //此处为Push_Recv
                    if(Init_Flag_Ser[send_id][key] == 0){
			if(msg->meta.push_pull_operation == -15)
			   Ser_Push_Recv[send_id][key].operation_count = -2;
			else
                           Ser_Push_Recv[send_id][key].operation_count = 0;
                        Ser_Push_Recv[send_id][key].time            = start;
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                           FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                           fprintf(file,"====================================================================================================================================\n");
                           fprintf(file,"==		 num_workers: = %d     num_servers: = %d     server_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			   fprintf(file,"==		 Worker_ID: = ");
			   for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			   }
			   fprintf(file,"\tServer_ID: =");
			   for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			   fprintf(file,"\n");
			   fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			   fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			   fprintf(file,"==     push =|\n");
			   fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			   fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			   fprintf(file,"==     pull =|\n");
			   fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			   fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			   fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			   fprintf(file,"=====================================================================================================================================\n");
                           fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                           fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Push_Recv[send_id][key].operation_count, send_id, 0, 0, start.tv_sec, start.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Push_Recv[send_id][key].operation_count, send_id, 0, 0, start.tv_sec, start.tv_usec, -1);//此时的依赖为-1，不存在依赖；
                            fclose(file);
                        }
                        Init_Flag_Ser[send_id][key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                    }//end Init_Flag == 0;
                    else{
                        dep_time = 1000000 * (start.tv_sec - (Ser_Pull_Send[send_id][key].time).tv_sec) + start.tv_usec - (Ser_Pull_Send[send_id][key].time).tv_usec;
                        Ser_Push_Recv[send_id][key].operation_count = Ser_Pull_Send[send_id][key].operation_count+1;
                        Ser_Push_Recv[send_id][key].time            = start;
                        FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                        fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Push_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-w%d\n",
                                record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Push_Recv[send_id][key].operation_count, send_id, 4, dep_time, start.tv_sec, start.tv_usec, Ser_Pull_Send[send_id][key].operation_count, send_id);//push_Recv依赖于整组pull_send操作的完成；依赖关系为push-on-pull
                        fclose(file);
                    }
                }//end Push_Recv
                else{
                //此为Pull_Recv
                    if(msg->meta.recv_key == -1){
                        if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file,"====================================================================================================================================\n");
                            fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    fprintf(file,"==		 Worker_ID: = ");
			    for(int w_num = 0; w_num < num_workers; w_num++){
			    	fprintf(file,"%d,",w_num);	
			    }
			    fprintf(file,"\tServer_ID: =");
			    for(int s_num = 0; s_num < num_servers; s_num++)
				fprintf(file,"%d,", s_num);
			    fprintf(file,"\n");
			    fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n");
			    fprintf(file,"==     push =|\n");
			    fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n");
			    fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n");
			    fprintf(file,"==     pull =|\n");
			    fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n");
			    fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    fprintf(file,"=====================================================================================================================================\n");
                            fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_TO_Workers\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                            fclose(file);
                        }
                        else{
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= SendCom_To_Workers\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation);//无需体现其依赖性
                            fclose(file);
                        }
                    }
                    else{
                        if(Init_Flag_Ser[send_id][key] == 0){
                            Ser_Pull_Recv[send_id][key].operation_count = 0;
                            Ser_Pull_Recv[send_id][key].time            = start;
                            if (record_count == 0) {//如果是刚开始，则在文件的第一部分写入相关内容的名称；
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file,"====================================================================================================================================\n");
                            	fprintf(file,"==		 num_workers: = %d     num_servers: = %d     worker_hostname:= %s            \n", num_workers, num_servers, host);//基础信息
			    	fprintf(file,"==		 Worker_ID: = ");
			    	for(int w_num = 0; w_num < num_workers; w_num++){
			    		fprintf(file,"%d,",w_num);	
			    	}
			   	fprintf(file,"\tServer_ID: =");
			    	for(int s_num = 0; s_num < num_servers; s_num++)
					fprintf(file,"%d,", s_num);
			    	fprintf(file,"\n");
			    	fprintf(file,"==     Dep_PP:  odd: push_operation   even: pull_operation  -15: Initialize Server  0: setup messages\n");
			    	fprintf(file,"==           |==  push_send_worker ----> push_recv_server\n ");
			    	fprintf(file,"==     push =|\n");
			    	fprintf(file,"==           |==  push_recv_worker <---- push_send_server\n ");
			    	fprintf(file,"==           |==  pull_send_worker ----> pull_recv_server\n ");
			    	fprintf(file,"==     pull =|\n");
			    	fprintf(file,"==           |==  pull_recv_worker <---- pull_send_server\n ");
			    	fprintf(file,"==	 OP_ID: key-operation_num-role  key: the index of parameters  role: (worker or server)\n");  
			    	fprintf(file,"              operation_num: the num of push or pull, every four num means an iteration (push_send, push_recv, pull_send, pull_recv)\n");
			    	fprintf(file,"=====================================================================================================================================\n");
                                fprintf(file, "ID\tsrc\tdst\tlength\t\tDep_PP\t\tOperation\t\tOP_ID\t\tdep_Type\t\tdTime\t\ttime_sec\t\ttime_usec\t\tIDdep\n");//共有10个参数，需要11个数据；
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n",
                                        record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Pull_Recv[send_id][key].operation_count, send_id, 0, 0, start.tv_sec, start.tv_usec, -1);//如果record_count值为0，必然不存在依赖，此时的依赖为-1，就表示不存在依赖；
                                fclose(file);
                            }
                            else{
				dep_time = 1000000*(start.tv_sec - (Ser_Push_Send[0][key].time).tv_sec) + start.tv_usec - (Ser_Push_Send[0][key].time).tv_usec;
                                FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                                fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d-w0\n",
                                        record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Pull_Recv[send_id][key].operation_count, send_id, 2, dep_time, start.tv_sec, start.tv_usec, key, Ser_Push_Send[0][key].operation_count);//此时的依赖为-1，不存在依赖；
                                fclose(file);
                            }
                            Init_Flag_Ser[send_id][key] = 1;//经过一次操作之后，falg的值置为1，在下一次执行同一个key值的push_send操作时，必然存在依赖；
                        }//end Init_Flag == 0;
                        else{
                            dep_time = 1000000 * (start.tv_sec - (Ser_Push_Send[send_id][key].time).tv_sec) + start.tv_usec - (Ser_Push_Send[send_id][key].time).tv_usec;
                            Ser_Pull_Recv[send_id][key].operation_count = Ser_Push_Send[send_id][key].operation_count+1;
                            Ser_Pull_Recv[send_id][key].time            = start;
                            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_record.txt", "a");
                            fprintf(file, "%d\t%d\t%d\t%d\t\t%d\t\tOP:= Pull_Recv_Server\t\t%d-%d-w%d\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d-%d-w%d\n",
                                    record_count, send_id, recv_id, send_bytes, msg->meta.push_pull_operation, key, Ser_Pull_Recv[send_id][key].operation_count, send_id, 2, dep_time, start.tv_sec, start.tv_usec, key, Ser_Push_Send[send_id][key].operation_count, send_id);//push_Recv依赖于整组pull_send操作的完成；依赖关系为push-on-pull
                            fclose(file);
                        }
                    }
                }//end Pull_Recv
                record_count++;
            }//end sender == worker
        }
    }
//******************************************** xym edit 2017-12-25 ****************************************************//
//                                                                                                                     //
//                                  This is used to distinguish scheduler from worker and server                       //
//                                                                                                                     //
//=====================================================================================================================//

  int SendMsg(const Message& msg) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
    SArray<Key> key_;//xym edit this 2017-12-29
    int key = 0;     
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;

    // send meta
    int meta_size; char* meta_buf;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno);
      return -1;
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;
    // send data
    for (int i = 0; i < n; ++i) {
      zmq_msg_t data_msg;
      SArray<char>* data = new SArray<char>(msg.data[i]);
//========================= xym edit 2017-12-21=========================//
/*      if(i == 0)
      {
          key_ = *((SArray<Key> *)(data));
          FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_push_pull.txt","a");
          fprintf(file,"SendMsg_Key: = %d\t\tMeta_Key: = %d\n",key_[0],msg.meta.recv_key);
          fclose(file);
	  key = key_[0];
      }
*/
//========================= xym edit 2017-12-21=========================//
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
      if (i == n - 1) tag = 0;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << n;
        return -1;
      }
      zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
//====================xym edit this 12-12 ========================//
//      Trace_Send(msg, send_bytes);
      Trace_Send_(msg, send_bytes, msg.meta.recv_key);
//====================xym edit this 12-11 ========================//
      return send_bytes;
  }

  int RecvMsg(Message* msg) override {
    msg->data.clear();
    int key = -5;
    size_t recv_bytes = 0;
    for (int i = 0; ; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to receive message. errno: "
                     << errno << " " << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;

      if (i == 0) {
        // identify
        msg->meta.sender = GetNodeID(buf, size);
        msg->meta.recver = my_node_.id;
        CHECK(zmq_msg_more(zmsg));
        zmq_msg_close(zmsg);
        delete zmsg;
      } else if (i == 1) {
        // task
        UnpackMeta(buf, size, &(msg->meta));
        zmq_msg_close(zmsg);
        bool more = zmq_msg_more(zmsg);
        delete zmsg;
        if (!more) break;
      } else {
        // zero-copy
        SArray<char> data;
        data.reset(buf, size, [zmsg, size](char* buf) {
            zmq_msg_close(zmsg);
            delete zmsg;
          });
	msg->data.push_back(data);
//======================== xym edit 12-21 ==================================//
/*        if(i == 2){
            key = *((int *)(&((msg->data)[0][0])));
            FILE *file = fopen("/home/hadoop/tensor/data_trace/trace_push_pull.txt","a");
            fprintf(file,"Recv_Msg_Key: = %d\t\tMeta_Key: = %d\n",key, msg->meta.recv_key);
            fclose(file);
        }
*/
//======================== xym edit 12-21 ==================================//
        if (!zmq_msg_more(zmsg)) { break; }
      }
    }
    
//===================== xym edit-12.01 ==================//
//    Trace_Recv(msg, recv_bytes);
    Trace_Recv_(msg, recv_bytes, msg->meta.recv_key);
//===================== xym edit-12-11 =================//
    return recv_bytes;
  }

 private:
  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  void *context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  void *receiver_ = nullptr;

//**************************** xym edit ****************************//
  /**
   * defined to record the trace -- xym edit 10.16
   */
    std::unordered_map<int, int> trace_;
    struct  timeval start;
    struct  timeval end;
    int record_count = 0;
    int record_sch   = 0;//用于记录与scheduler节点有关的SendMsg和RecvMsg操作，实际上和scheduler有关的通信操作都不是由于push和pull操作产生的；
    int scheduler_count = 0;
    int dep_record;
    std::mutex xu_;
    
//用于记录worker在分布式训练过程中的trace
    std::unordered_map<int, Trace_> Trace_Push_Send;
    std::unordered_map<int, Trace_> Trace_Push_Recv;
    std::unordered_map<int, Trace_> Trace_Pull_Send;
    std::unordered_map<int, Trace_> Trace_Pull_Recv;
    std::unordered_map<int, int> Init_Flag_Wor;//在写入文件时，如果key值对应的Flag值为0，则不存在依赖，否则都有对同一可以值其他操作的依赖；
//用于记录server在分布式训练过程中的trace
    std::unordered_map<int, Trace_> Ser_Push_Recv[2]; //此处2的值为分布式执行过程中的worker_num值，
    std::unordered_map<int, Trace_> Ser_Push_Send[2];
    std::unordered_map<int, Trace_> Ser_Pull_Recv[2];
    std::unordered_map<int, Trace_> Ser_Pull_Send[2];
    std::unordered_map<int, int> Init_Flag_Ser[2];
//**************************** xym edit ****************************//
    
};
}  // namespace ps

#endif  // PS_ZMQ_VAN_H_

