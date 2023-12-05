import threading
import datetime
import subprocess
import socket
import sys
import os
import time
import random
import json

#########hard code area
server_nums = [i for i in range(1, 11)]
host_name = 'fa23-cs425-73{}.cs.illinois.edu'
machine_2_ip = {i: 'fa23-cs425-73{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
machine_2_ip[10] = 'fa23-cs425-7310.cs.illinois.edu'                                            #host domain name of machine 10
root = '/home/jsku2/MP1/logs/'    #log file path name 
mp2_log_path = '/home/jsku2/mp2_intro_log'
msg_format = 'utf-8'                #data encoding format of socket programming
logs = [root]                       #if need to handle multi log files on a single machine
gossiping_sockets = {}   #record all socket for gossiping
gossiping_recv_port = 5003 #temp
introducer_port = 5004
gossiping_timestep = {}
gossiping_threadpool = {}
# gossiping_timeout = 0.3
membership_list = {} #dict() dict([domain_name, hearbeat, localtime, status])
host_domain_name = socket.gethostname() 
machine_id = int(host_domain_name[13:15])
T_fail = 4.9
T_cleanup = 2
# heartbeatrate = 0.3
# leave_status = True # check if leave_status is flagged
# suspicion_mode = False # this may need to be declared global everywhere
#########


class logging():
    def __init__(self):
        pass
    def info(self, log):
        print("[LOGGING INFO {}]: ".format(datetime.datetime.now()), log)
    def debug(self, log):
        print("[LOGGING DEBUG {}]: ".format(datetime.datetime.now()), log)
    def error(self, log):
        _, _, exc_tb = sys.exc_info()
        print("[LOGGING ERROR {}, in line {}]: ".format(datetime.datetime.now(), exc_tb.tb_lineno), log)
    def writelog(self, log):
        print("[LOGGING WRITE {}]: {}\n".format(datetime.datetime.now(), log))
        

logger = logging()
SUCCESS = True
FAILURE = False


def gossiping_send(addr):
    # sending membersjip list back to a join server
    logger.info("Send membership list back to the join server {}".format(str(addr)))
    msg = json.dumps(membership_list)
    msg = msg.encode(msg_format)
    gossiping_sockets['introducer'].sendto(msg, (addr[0], gossiping_recv_port))


def update_membership(new_list): #update per recieve new memebership list

    for domain_name, info in new_list.items():
        if domain_name not in membership_list:
            # new join
            # initialize/modify membership list
            membership_list[domain_name] = dict(info)
            membership_list[domain_name]['timestamp'] = time.time() 
            logger.writelog("server {} joins the membership list on local time {} (s)".format(domain_name, membership_list[domain_name]['timestamp']))

        elif info['status'] in ['Leave', 'Failure']:
            # if certain server leave or fail, move out the membership list
            logger.writelog("server {} {} is moved out membership list".format(domain_name, info['status']))
            del membership_list[domain_name]
        
        elif membership_list[domain_name]['heartbeat'] <= info['heartbeat']: # update based on heartbeat
            # update local time
            membership_list[domain_name]['status'] = info['status']
            membership_list[domain_name]['heartbeat'] = info['heartbeat']
            if info['status'] == 'Join':
                membership_list[domain_name]['timestamp'] = time.time()
                
    for domain_name, info in dict(membership_list).items():
        # if certain server leave or fail, move out the membership list
        if info['status'] in ['Leave', 'Failure']:
            logger.writelog("server {} {} timeout, move out membership list".format(domain_name, info['status']))
            del membership_list[domain_name]

        


def gossiping_recv():
    # recv membership list from servers
    server_id = machine_id
    logger.info("gossiping recv: server id is {}".format(server_id))
    while True:
        # error handling
        try:
            data, addr = gossiping_sockets["introducer"].recvfrom(4096) # data should be non
            logger.info("recv ping from server {}".format(addr))
            data = data.decode(msg_format)
            tmp_membership_list = json.loads(data)
            new_join = len(tmp_membership_list) == 1 
            # update local membership list based on recieved membership list
            update_membership(tmp_membership_list)
            # if only a member inside memberlist, send it a memberlist
            if new_join:
                gossiping_send(addr)
        except Exception as e:
            logger.error("Unexpected error occur during recv membership: {}".format(str(e)))

# def gossiping_timestamp_handling(): #increase heartbeat and failure/cleanup detection and check leave status
    
#     last_time_stamp = time.time()
#     while True:
#         current_time = time.time()
#         # increase heartbeat
#         # add second condition to ensure status isn't leave or failure
#         # if current_time-membership_list[host_domain_name]['timestamp']>heartbeatrate and membership_list[host_domain_name]['status'] == 'Join':
#         #    membership_list[host_domain_name]['timestamp'] = current_time
#         #    membership_list[host_domain_name]['heartbeat']+=1     
            
#         # failure node and cleanup
#         for domain_name, info in dict(membership_list).items():
#             if current_time-info['timestamp'] > T_fail and info['status'] == 'Join':
#                 logger.writelog("server {} Failure timeout, label as failure".format(domain_name))
#                 membership_list[domain_name]['status'] = 'Failure'
#                 membership_list[domain_name]['timestamp'] = current_time
#             elif current_time-info['timestamp'] > T_cleanup and info['status'] == 'Failure':
#                 logger.writelog("server {} CleanUp timeout, move out membership list".format(domain_name))
#                 del membership_list[domain_name]
#             elif current_time-info['timestamp'] > T_cleanup and info['status'] == 'Leave':
#                 logger.write(f"server {domain_name} leaves the membership list on local time {current_time}")
#                 del membership_list[domain_name]
        

def gossiping():
    #UDP
    #declarations
    global leave_status
    logger.info("set up sockets for host gossiping")
    #Initialize host socket
    sock = socket.socket(socket.AF_INET, # Internet
                          socket.SOCK_DGRAM) # UDP
    sock.bind((host_domain_name, introducer_port))
    gossiping_sockets['introducer'] = sock
    #initialize membership list

    logger.info("runs pinging/pinged thread")
    gossiping_threadpool['gossiping_pinged'] = threading.Thread(target=gossiping_recv)
    gossiping_threadpool['gossiping_pinged'].start()
    # gossiping_threadpool['gossiping_timestamp_handling'] = threading.Thread(target=gossiping_timestamp_handling)
    # gossiping_threadpool['gossiping_timestamp_handling'].start()
    while True:
        if not gossiping_threadpool['gossiping_pinged'].is_alive():
            gossiping_threadpool['gossiping_pinged'].join()
            gossiping_threadpool['gossiping_pinged'] = threading.Thread(target=gossiping_recv)
            gossiping_threadpool['gossiping_pinged'].start()
        # if not gossiping_threadpool['gossiping_timestamp_handling'].is_alive():
        #     gossiping_threadpool['gossiping_timestamp_handling'].join()
        #     gossiping_threadpool['gossiping_timestamp_handling'] = threading.Thread(target=gossiping_timestamp_handling)
        #     gossiping_threadpool['gossiping_timestamp_handling'].start()
        time.sleep(2) #avoid context switch


if __name__ == "__main__":
    
    gossiping_main = threading.Thread(target=gossiping)
    gossiping_main.start()
    while True:
        logger.debug(f"Membership list: {str(membership_list)}")
        if not gossiping_main:
            gossiping_main.join()
            gossiping_main = threading.Thread(target=gossiping)
            gossiping_main.start()
        time.sleep(3) # avoid thread context switch



