import socket
import asyncio
import datetime
import logging
import threading
import sys
msg_format = 'utf-8'

class logging():
    def info(self, log):
        print("[LOGGING INFO {}]: ".format(datetime.datetime.now()), log)
    def debug(self, log):
        print("[LOGGING DEBUG {}]: ".format(datetime.datetime.now()), log)
    def error(self, log):
        _, _, exc_tb = sys.exc_info()
        print("[LOGGING ERROR {}, in line {}]: ".format(datetime.datetime.now(), exc_tb.tb_lineno), log)

logger = logging()

class Client():

    def __init__(self):
        self.result = {}

    def send_query(self, host, port, msg, server_id):
        #use socket programming to send query to all servers, time out with 5 seconds
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5)
        try:
            # Connect to the target server
            client_socket.connect((host, port))
            logger.info("Succefully connected to the server")
            # Send a message to the server
            client_socket.send(msg.encode(msg_format))
            # get message back from server
            log = ''
            while True:
                tmp = client_socket.recv(128*1024).decode(msg_format)
                if not tmp:
                    break
                log+=tmp
            # Close the socket
            client_socket.close()
            # log the whole matched lines
            #logger.info(f'Result at server{str(server_id)}: '+log.strip())
            self.result[server_id] = log.strip()
            return log
        
        except socket.timeout:
            logger.error(f"Connection attempt timed out (5 seconds) at server {str(server_id)}")
            return str(-1)
        except ConnectionRefusedError:
            logger.error(f"Connection refused, server {str(server_id)} may not be running")
            self.result[server_id] = str(-1)
            return str(-1)
        except Exception as e:
            logger.error(f"Unexpected error occurred ,server_id: {str(server_id)}: {str(e)}")
            self.result[server_id] = str(-1)
            return str(-1)

### MODIFY ACCORDINGLY TO SERVER ###
host_name = 'fa23-cs425-73{}.cs.illinois.edu'
machine_2_ip = {i: 'fa23-cs425-73{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}   #host domain names of machnine 1~9 
machine_2_ip[10] = 'fa23-cs425-7310.cs.illinois.edu'                                           #host domain name of machine 10
port = 5001
filename = 'MP1/logs/vm{}.log' #log file path name format will be server ID

### FETCH PER FOR EACH SERVER ###
def get_each_server(servers, query):
    #sending query to every machine
    #thread_pool: dictionary to record each thread's status
    thread_pool = {}
    client = Client()

    for server_id, host in servers.items():
        #multi threading, trigger working thread per server machine
        #label each thread with task_id (by server id and query)
        #thread_pool: a dictionary to keep track of all working threads
        task_id = str(server_id)+'_'+query
        thread_pool[task_id] = threading.Thread(target=client.send_query, args=[host, port, query, server_id])
        thread_pool[task_id].start()
    
    #multi thread programming, join finished threads
    while thread_pool:
        end_tasks = []
        for task in thread_pool:
            if not thread_pool[task].is_alive():
                end_tasks.append(task)
        for task in end_tasks:
            thread_pool[task].join()
            del thread_pool[task]
            # get server_num
            if task[1] != '_':
                server_num = task[:2]
            else:
                server_num = task[:1]
            logger.info("Finish task:[{}] at server{}".format(task, server_num))
    #sum up results collection from each machine
    if query[0] == 'c':
        sum_all = 0
        #print(client.result)
        for v in client.result.values():
            if int(v) > 0:
                sum_all+=int(v)
        logger.info('Individual result for each VM: '+str(client.result))
        logger.info('all results: ' + str(sum_all))
    else:
        #sum up results collection from each machine
        ans = {}
        sum_all = 0

        for k, v in client.result.items():
            if v == '' or v == '-1':
                ans[k] = 0
            else:
                tmp = len(v.strip().split('\n'))
                sum_all += tmp
                ans[k] = tmp
            # sum_all+=v.strip()
        # logger.info('Individual result for each VM: '+str(client.result))
        # logger.info('total line counts: ' + sum_all)
        logger.info('Matched lines for each machine '+ str(ans))
        logger.info('Total lines in all machines: ' + str(sum_all))

    


if __name__ == "__main__":

    # main function, trigger main working thread get_each_server
    try:
        query = sys.argv[1]
        query_type = sys.argv[2]
        query = query_type+query
        logger.info("Query :"+query)
    except Exception as e:
        logger.error("input format error")
    
    if query_type in ('a', 'c'):
        get_each_server(machine_2_ip, query)
    else:
        logger.info("Invalid flag, query type should be either a or c")