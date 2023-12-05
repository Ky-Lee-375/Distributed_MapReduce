import threading
import datetime
import subprocess
import socket
import sys
import os
import time
import random
import json
import select
import collections
import multiprocessing
from collections import Counter, deque

#########hard code area
server_nums = [i for i in range(1, 11)]
host_name = 'fa23-cs425-73{}.cs.illinois.edu'
machine_2_ip = {i: 'fa23-cs425-73{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
machine_2_ip[10] = 'fa23-cs425-7310.cs.illinois.edu'                                            #host domain name of machine 10
root = '/home/jsku2/MP1/logs/'    #log file path name 
# mp2_log_path = '/home/jsku2/mp2_server_log'
mp2_log_path = '/home/jsku2/mp2_server_log'
msg_format = 'utf-8'                #data encoding format of socket programming
logs = [root]                       #if need to handle multi log files on a single machine
gossiping_sockets = {}   #record all socket for gossiping
gossiping_recv_port = 5003 #temp
gossiping_send_port = 5005
gossiping_send_intro_port =5006
introducer_port = 5004
gossiping_timestep = {}
gossiping_threadpool = {}
gossiping_timeout = 0.1
sock_timeout = 0.08
membership_list = {} #dict() dict([domain_name, heartbeat, localtime, status])
host_domain_name = socket.gethostname() 
machine_id = int(host_domain_name[13:15])
T_fail = 4
T_cleanup = 2
heartbeatrate = 0.07
leave_status = True # check if leave_status is flagged
suspicion_mode = False # this may need to be declared global everywhere
suspicion_timeout = 2
msg_drop_rate = 0.0
memberlist_lock = threading.Lock()
recieve_time = 0
failure_time = 0
status_join = set()
status_suspicious = set()
gossiping_start_time = None
#########



class logging():
    def __init__(self):
        with open(mp2_log_path, 'w+') as fd:
            #create an empty log file
            pass
    def info(self, log):
        pass
        #print("[LOGGING INFO {}]: ".format(datetime.datetime.now()), log)
    def debug(self, log):
        pass
        #print("[LOGGING DEBUG {}]: ".format(datetime.datetime.now()), log)
    def error(self, log):
        _, _, exc_tb = sys.exc_info()
        pass
        #print("[LOGGING ERROR {}, in line {}]: ".format(datetime.datetime.now(), exc_tb.tb_lineno), log)
    def writelog(self, log, stdout=True):
        return
        if stdout:
            print("[LOGGING WRITE {}]: {}".format(datetime.datetime.now(), log))
        with open(mp2_log_path, 'a+') as fd:
            print("[LOGGING WRITE {}]: {}".format(datetime.datetime.now(), log), file = fd)
        #self.fd.write(log)

class FailDetector():

    def __init__(self):
        #########hard code area
        self.logger = logging()
        self.server_nums = [i for i in range(1, 11)]
        self.host_name = 'fa23-cs425-73{}.cs.illinois.edu'
        self.machine_2_ip = {i: 'fa23-cs425-73{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
        self.machine_2_ip[10] = 'fa23-cs425-7310.cs.illinois.edu'                                            #host domain name of machine 10
        self.root = '/home/jsku2/MP1/logs/'    #log file path name 
        # mp2_log_path = '/home/jsku2/mp2_server_log'
        self.mp2_log_path = '/home/jsku2/mp2_server_log'
        self.msg_format = 'utf-8'                #data encoding format of socket programming
        self.logs = [root]                       #if need to handle multi log files on a single machine
        self.gossiping_sockets = {}   #record all socket for gossiping
        self.gossiping_recv_port = 5003 #temp
        self.gossiping_send_port = 5005
        self.gossiping_send_intro_port =5006
        self.introducer_port = 5004
        self.gossiping_timestep = {}
        self.gossiping_threadpool = {}
        self.gossiping_timeout = 0.1
        self.sock_timeout = 0.08
        self.membership_list = {} #dict() dict([domain_name, heartbeat, localtime, status])
        self.host_domain_name = socket.gethostname() 
        self.machine_id = int(host_domain_name[13:15])
        self.T_fail = 4
        self.T_cleanup = 2
        self.heartbeatrate = 0.07
        self.leave_status = True # check if leave_status is flagged
        self.suspicion_mode = False # this may need to be declared global everywhere
        self.suspicion_timeout = 2
        self.msg_drop_rate = 0.0
        self.memberlist_lock = threading.Lock()
        self.recieve_time = 0
        self.failure_time = 0
        self.status_join = set()
        self.status_suspicious = set()
        self.total_bytes = 0
        self.gossiping_start_time = None
        self.failure_queue = deque()
        self.filelocation_intro_queue = deque()
        #########


    def gossiping_timestamp_handling(self): # increase heartbeat and failure/cleanup detection and check leave status
        
        # host left, return
        self.memberlist_lock.acquire()
        
        if self.host_domain_name not in self.membership_list:
            self.logger.info("host server left, terminate gossip main thread")
            self.memberlist_lock.release()
            return
        
        current_time = time.time()

        if current_time-self.membership_list[self.host_domain_name]['timestamp']>self.heartbeatrate and \
                self.membership_list[self.host_domain_name]['status'] not in ['Failure', 'Leave']:
                # host update heartbeat based on heartbeat rate
            self.membership_list[self.host_domain_name]['timestamp'] = current_time
            self.membership_list[self.host_domain_name]['heartbeat']+=1     
        
        # check host leave status and start T_cleanup count
        if self.leave_status == True and self.membership_list[self.host_domain_name]['status'] == 'Join':
            self.membership_list[self.host_domain_name]['status'] = 'Leave'
            self.membership_list[self.host_domain_name]['timestamp'] = current_time
        
        # failure node and cleanup
        for domain_name, info in dict(self.membership_list).items():
            
            # change certain member's status from join->suspcious
            if self.suspicion_mode and info['status'] == 'Join' and time.time()-info['timestamp'] > self.suspicion_timeout:
                self.logger.writelog("server {} Suspicion timeout: {} second, label as suspicion".format(domain_name, time.time()-info['timestamp']))
                self.status_suspicious.add(domain_name)
                self.membership_list[domain_name]['status'] = 'Suspicion'
                self.membership_list[domain_name]['timestamp'] = time.time()
            
            # change certain member's status from suspcious-> fail
            elif self.suspicion_mode and info['status'] == 'Suspicion' and time.time()-info['timestamp'] > self.T_fail:
                self.failure_time += 1
                self.logger.writelog("server {} Failure timeout: {} second, Suspicion label as failure, False Positive Rate: {}".format(domain_name, time.time()-info['timestamp'], float(self.failure_time/self.recieve_time)))
                self.membership_list[domain_name]['status'] = 'Failure'
                self.membership_list[domain_name]['timestamp'] = time.time()
            
            # change certain member's status from join->failure
            elif info['status'] == 'Join' and time.time()-info['timestamp'] > self.T_fail:
                self.failure_time+=1
                self.logger.writelog("server {} Failure timeout: {} second, label as failure. False positive rate: {}".format(domain_name, time.time()-info['timestamp'], float(self.failure_time/self.recieve_time)))
                self.status_join-={domain_name}
                self.membership_list[domain_name]['status'] = 'Failure'
                self.failure_queue.append(domain_name)
                self.membership_list[domain_name]['timestamp'] = time.time()
                if self.recieve_time > 0:
                    self.logger.debug("False positive prob is : {}".format(self.failure_time/self.recieve_time))
            
            # clean up failure node after timeout
            elif info['status'] == 'Failure' and time.time()-info['timestamp'] > self.T_cleanup:
                self.logger.writelog("server {} CleanUp timeout, move out membership list".format(domain_name))
                del self.membership_list[domain_name]
            
            # clean up leaving node after timeout
            elif info['status'] == 'Leave' and time.time()-info['timestamp'] > self.T_cleanup:
                self.logger.writelog(f"server {domain_name} leaves the membership list on local time {time.time()}")
                del self.membership_list[domain_name]
        
        self.memberlist_lock.release()

    def filter_memberlist(self, suspicion = False): # filter out infomation that is need to send in membership list
        if suspicion: # add heart beat to notify alive
            self.membership_list[self.host_domain_name]['heartbeat'] += 1
        
        ret = {}
        for server, info in dict(self.membership_list).items():
            ret[server] = dict()
            ret[server]['heartbeat'] = info['heartbeat']
            ret[server]['status'] = info['status']
        return ret

    def send(self, sock, msg, ping_server, gossiping_recv_port): # funcion for actually sendto
        send_size = sock.sendto(msg, (ping_server, gossiping_recv_port))
        return

    def gossiping_send(self): # logic of sending gossiping messages per time frame

        
        while True:
            # host left, return
            if self.host_domain_name not in self.membership_list:
                self.logger.info("host server left, terminate send thread")
                return

            # trigger updating membership list's state per time frame before sending out
            self.gossiping_timestamp_handling()

            # data preprocessing before sending out
            self.memberlist_lock.acquire()
            ret = self.filter_memberlist()
            msg = json.dumps(ret)
            self.memberlist_lock.release()
            msg = msg.encode(msg_format)

            # list for randomg gossiping target
            ping_candidates = list(self.status_join)

            try:
                # gossiping if there is at least another member in membership list
                if len(ping_candidates) >= 1 and self.msg_drop_rate < random.random(): 
                    ping_server = random.choice(ping_candidates)
                    sock = socket.socket(socket.AF_INET, 
                            socket.SOCK_DGRAM)
                    # trigger sending function
                    p = multiprocessing.Process(target=self.send, name="send", args=(sock, msg, ping_server, self.gossiping_recv_port))
                    self.total_bytes+=len(msg)
                    p.start()
                    time.sleep(self.sock_timeout)
                    if p.is_alive():
                        # if socket blocked too long, kill and execute again
                        self.total_bytes-=len(msg)
                        self.logger.info("Pinging server {} Timeout".format(ping_server))
                        p.terminate()
                    p.join()

                # sending current membership list to introducer occasionally
                if random.random() > 0.8:
                    sock = socket.socket(socket.AF_INET, # Internet
                            socket.SOCK_DGRAM)# UDP 
                    p = multiprocessing.Process(target=self.send, name="send", args=(sock, msg, self.machine_2_ip[1], self.introducer_port))
                    p.start()
                    time.sleep(self.sock_timeout)
                    if p.is_alive():
                        self.logger.info("Pinging introducer {} Timeout".format(self.machine_2_ip[1]))
                        p.terminate()
                    p.join()

            except Exception as e:
                self.logger.error("Unexpect erro occur duing sending: {}".format(str(e)))
                continue
            
            # sleep till next sending
            time.sleep(self.gossiping_timeout)
            
    def update_membership(self, new_list, addr = None): #update per recieve new memebership list
        
            
        #global status_join, total_bytes
        self.memberlist_lock.acquire()
        
        try:
            for domain_name, info in new_list.items():
                # Update membership list based on different states

                if info['status'] == 'Failure': # state failure, do nothing
                    continue

                elif info['status'] =='Leave': # some member leave the group

                    # host had already moved the member out from list and clean up
                    if domain_name not in self.membership_list: 
                        continue

                    # host had labeled the member as "Leave" but haven't clean up
                    elif self.membership_list[domain_name]['status'] == info['status']: #
                        continue

                    # first time the host recieve certain member leave group, update membership list
                    else:
                        self.status_join-={domain_name}
                        self.membership_list[domain_name]['status'] = info['status']
                        self.membership_list[domain_name]['heartbeat'] = info['heartbeat']
                        self.membership_list[domain_name]['timestamp'] = time.time()

                elif info['status'] == 'Suspicion': # state suspicion
                    if domain_name == self.host_domain_name: # notify still alive
                        ret = self.filter_memberlist(suspicion = True)
                        msg = json.dumps(ret)
                        msg = msg.encode(msg_format)
                        sock = socket.socket(socket.AF_INET, 
                            socket.SOCK_DGRAM)
                        p = multiprocessing.Process(target=self.send, name="send", args=(sock, msg, addr[0], self.introducer_port))
                        self.total_bytes+=len(msg)
                        p.start()
                        time.sleep(self.sock_timeout)
                        if p.is_alive():
                            # if socket blocked too long, kill and execute again
                            self.total_bytes-=len(msg)
                            self.logger.info("Pinging server {} Timeout".format(addr))
                            p.terminate()
                        p.join()

                else: # state Join

                    # new join member, initialize/modify membership list
                    if domain_name not in self.membership_list:
                        self.status_join.add(domain_name)
                        self.filelocation_intro_queue.append(domain_name)
                        self.membership_list[domain_name] = dict(info)
                        self.membership_list[domain_name]['timestamp'] = time.time()
                        self.logger.writelog("server {} joins the membership list on local time {} (s)".format(domain_name, self.membership_list[domain_name]['timestamp']))

                    # old member, update when heartbeat > local membership list's heartbeat
                    elif self.membership_list[domain_name]['heartbeat'] < info['heartbeat']:# and info['status'] != 'Failure': #need update                 
                        self.status_join.add(domain_name)
                        self.membership_list[domain_name]['status'] = 'Join'
                        self.membership_list[domain_name]['heartbeat'] = info['heartbeat']
                        self.membership_list[domain_name]['timestamp'] = time.time()

        except Exception as e:
            self.logger.error("Unexpect error occur during updating: {}".format(str(e)))
        self.memberlist_lock.release()
        
        


    def gossiping_recv(self):
        # busy waiting for recieving gossiping messages
        server_id = machine_id
        buffer, dicts = "", []
        #global recieve_time, total_bytes

        while True:
            # host left, return
            if self.host_domain_name not in self.membership_list:
                self.logger.info("host server left, terminate recv thread")
                return

            try:
                # i/o multiplexing
                readable, _, _ = select.select([self.gossiping_sockets['recv']], [], [], 0)
                if len(readable) == 0:
                    continue
                data, addr = self.gossiping_sockets['recv'].recvfrom(1536)
                self.total_bytes += len(data)
                self.recieve_time+=1
                data = data.decode(msg_format)
                tmp_membership_list = json.loads(data)
                # update local membership list based on recieved membership list
                self.update_membership(tmp_membership_list, addr)

            except Exception as e:
                self.logger.error("Unexpected error occur during recv membership: {}".format(str(e)))

            
    def notify_introducer(self): # requesting membership list from introducer while first join

        while True:
            try:
                self.logger.info("Send membership list to introducer when first join")
                # send local membership list to introducer, which should contain only host itself
                msg = json.dumps(self.membership_list)
                sock = socket.socket(socket.AF_INET, 
                            socket.SOCK_DGRAM)
                msg = msg.encode(msg_format)
                sock.sendto(msg, (self.machine_2_ip[1], self.introducer_port))
                
                # recieving membership list back from introducer
                self.logger.info("Recv membership list from introducer when first join")
                data, addr = self.gossiping_sockets['recv'].recvfrom(4096)
                self.logger.info("Recv membership list from introducer: {}".format(data))
                data = data.decode(msg_format)
                tmp_membership_list = json.loads(data)

                # update local membership list based on recieved membership list
                self.update_membership(tmp_membership_list)
                self.membership_list[self.host_domain_name]['timestamp'] = time.time()
                self.logger.info("Updated membership list: {}".format(str(self.membership_list)))
                break

            except Exception as e:
                self.logger.error("Unexpect error occur during notifying introducer, re-send: {}".format(str(e)))

    def gossiping(self):
        # This is the starting point of gossiping, this fuction will 
        # (1) initialize recieving socket, 
        # (2) create threads for recieving and sending 

        #declarations
        #global leave_status, sock_timeout, total_bytes, gossiping_start_time
        self.total_bytes = 0
        self.gossiping_start_time = time.time()
        self.logger.info("set up sockets for host gossiping")


        #Initialize host socket for recieving
        sock = socket.socket(socket.AF_INET, # Internet
                            socket.SOCK_DGRAM) # UDP
        sock.bind((self.host_domain_name, self.gossiping_recv_port))
        self.gossiping_sockets['recv'] = sock

        #initialize membership list
        self.membership_list[self.host_domain_name] = dict()
        self.membership_list[self.host_domain_name]['heartbeat'] = 0
        self.membership_list[self.host_domain_name]['timestamp'] = time.time()
        self.membership_list[self.host_domain_name]['status'] = 'Join'

        # notify introducer, get the current membership list maintain by introducer
        self.notify_introducer()

        # trigger sending and recieving threads
        self.logger.info("runs pinging/pinged thread")
        self.gossiping_threadpool['gossiping_pinging'] = threading.Thread(target=self.gossiping_send)
        self.gossiping_threadpool['gossiping_pinging'].start()
        self.gossiping_threadpool['gossiping_pinged'] = threading.Thread(target=self.gossiping_recv)
        self.gossiping_threadpool['gossiping_pinged'].start()


    def start_gossiping(self):
        self.leave_status = False
        self.suspicion_mode = False
        self.T_fail = 4
        self.gossiping()


