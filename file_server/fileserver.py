import threading
import datetime
import subprocess
import base64
import socket
import os
import sys
import csv
import time
import random
import json
import requests
import rpyc
import re
from rpyc.utils.server import ThreadedServer
import queue
from collections import Counter, deque, defaultdict
sys.path.insert(0, '../server')
from server import FailDetector

#########hard code area
server_nums = [i for i in range(1, 11)]
host_name = 'fa23-cs425-73{}.cs.illinois.edu'
machine_2_ip = {i: 'fa23-cs425-73{}.cs.illinois.edu'.format('0'+str(i)) for i in range(1, 10)}  #host domain names of machnine 1~9
machine_2_ip[10] = 'fa23-cs425-7310.cs.illinois.edu'                                            #host domain name of machine 10
msg_format = 'utf-8'                #data encoding format of socket programming
filelocation_list = {} # sdfs_filename: [ips which have this file]
host_domain_name = socket.gethostname() 
machine_id = int(host_domain_name[13:15])
file_sender_port = 5007
file_reciever_port = 5008
file_leader_port = 5009
file_sockets = {}
leader_queue = list()
schedule_counter = defaultdict(lambda : [0,0,0,0]) # schedule_counter = {'sdfsfilename':[R_count, W_count, R_pre, W_pre]}
mp3_log_path = '/home/jsku2/MP3_log'
put_ack = defaultdict(list)
delete_ack = defaultdict(list)
container_queue = queue.Queue()
node_task_map = {}
node_task_juice = {}
maple_out_list = []
juice_out_list = []
current_maple_job = {}
#########

fail_detector = FailDetector()

class logging():
    def __init__(self):
        with open(mp3_log_path, 'w+') as fd:
            #create an empty log file
            pass
    def info(self, log):
        print("[LOGGING INFO {}]: ".format(datetime.datetime.now()), log)
    def debug(self, log):
        print("[LOGGING DEBUG {}]: ".format(datetime.datetime.now()), log)
    def error(self, log):
        _, _, exc_tb = sys.exc_info()
        print("[LOGGING ERROR {}, in line {}]: ".format(datetime.datetime.now(), exc_tb.tb_lineno), log)
    def writelog(self, log, stdout=True):
        if stdout:
            print("[LOGGING WRITE {}]: {}".format(datetime.datetime.now(), log))
        with open(mp3_log_path, 'a+') as fd:
            print("[LOGGING WRITE {}]: {}".format(datetime.datetime.now(), log), file = fd)
        #self.fd.write(log)

logger = logging()
SUCCESS = True
FAILURE = False

class MyRPyCService(rpyc.Service):
    def exposed_execute_command(self, command):
        try:
            output = subprocess.check_output(command, shell=True)
            return output.decode('utf-8')
        except subprocess.CalledProcessError as e:
            return f"Error: {str(e)}"

def rpyc_server_thread():
    server = ThreadedServer(MyRPyCService, port=18812)  # Choose an appropriate port
    server.start()


def send_packet(dest, http_packet, port, request_type = None):
    """
        This function sends the http_packet to the destinations
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        sock.connect((dest, port))
        sock.send(http_packet)
        sock.close()
        print(f"Send_Packet success from {str(host_domain_name)} to {str(dest)}")
        return True
   
    except Exception as e:
        print(f"Connection Error while {request_type} at {str(dest)}, {str(e)}")
        return False


def send(http_packet, request_type, to_leader, replica_ips=None):

    """
        This function handles sends based on different request types/number of destinations
    """
    http_packet = json.dumps(http_packet)
    http_packet = http_packet.encode(msg_format)

    if not to_leader:

        if request_type in ['put', 'delete']:
            for dest in replica_ips:
                send_packet(dest, http_packet, file_reciever_port, request_type)

        elif request_type == 'rereplicate':
            for dest in replica_ips:
                return send_packet(dest, http_packet, file_reciever_port, request_type)
        
        elif request_type in ['get', 'finish_ack']:
            # only need to fetch first one
            print("***** request_type in ['get', 'finish_ack']")
            send_packet(replica_ips[0], http_packet, file_reciever_port, request_type)
        
        # request_type == 'update'
        elif request_type == 'update':
            for dest in dict(fail_detector.membership_list).keys():
                if dest != host_domain_name:
                    send_packet(dest, http_packet, file_reciever_port, request_type)
        elif request_type == "display":
            logger.info(request_type)
            send_packet(replica_ips[0], http_packet, file_reciever_port, request_type)
        else:
            print(f"REQUEST TYPE WRONG IN SEND ERROR!! {str(request_type)}")
    
    # send from user to leader
    else:
        leader_id = min(fail_detector.membership_list.keys())
        send_packet(leader_id, http_packet, file_leader_port, request_type)
    

def put_file(http_packet):
    """
        This function puts the file from local to designated sdfs servers(4 servers)
    """
    local = http_packet['local_filename']
    sdfs = http_packet['sdfs_filename']
    source = http_packet['request_source']
    print(f"From {str(source)} to {str(host_domain_name)} for {str(sdfs)}")
    cmd = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null jsku2@{source}:{local} /home/jsku2/MP3_FILE/{sdfs}'
    try:
        result = subprocess.check_output(cmd, shell=True)
        logger.info(f"Complete {str(http_packet)} ")
        return_packet = {}
        return_packet['task_id'] = http_packet['task_id']
        return_packet['request_type'] = 'put_ack'
        return_packet['request_source'] = http_packet['request_source']
        return_packet['sdfs_filename'] = http_packet['sdfs_filename']
        return_packet['replica_ip'] = host_domain_name
        send(return_packet, 'put_ack', True)

    except Exception as e:
        logger.error(f"Command: {cmd}, Error: {str(e)}")


def get_file(http_packet):
    """
    Server receieve get request, need to send file back to local
    Request need to have local, sdfs_file stored at {sdfs}
    """
    local = http_packet['local_filename']
    sdfs = http_packet['sdfs_filename']
    source = http_packet['request_source']
    logger.info(f"local: {local}")
    logger.info(f"sdfs: {sdfs}")
    logger.info(f"source: {source}")

    cmd = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/MP3_FILE/{sdfs} jsku2@{source}:{local}'
    try:
        result = subprocess.check_output(cmd, shell=True)
        logger.info(f"Complete {str(http_packet)} ")
        return_packet = {}
        return_packet['task_id'] = http_packet['task_id']
        return_packet['request_type'] = 'get_ack'
        return_packet['request_source'] = http_packet['request_source']
        return_packet['sdfs_filename'] = http_packet['sdfs_filename']
        return_packet['replica_ip'] = host_domain_name
        send(return_packet, 'get_ack', True)
    except Exception as e:
        logger.error(f"Command: {cmd}, Error: {str(e)}")

def delete_file(http_packet):
    """
        This function deletes the file at current SDFS folder (4 servers executed)
    """
    sdfs = http_packet['sdfs_filename']
    cmd = f'rm /home/jsku2/MP3_FILE/{sdfs}'
    try:
        result = subprocess.check_output(cmd, shell=True)
        logger.info(f"Complete {str(http_packet)} ")
        return_packet = {}
        return_packet['task_id'] = http_packet['task_id']
        return_packet['request_type'] = 'delete_ack'
        return_packet['request_source'] = http_packet['request_source']
        return_packet['sdfs_filename'] = http_packet['sdfs_filename']
        return_packet['replica_ip'] = host_domain_name
        send(return_packet, 'get_ack', True)

    except Exception as e:
        logger.error(f"Command: {cmd}, Error: {str(e)}")

def update_file(http_packet):
    """
        This function updates the file table for put/delete commands
    """
    try:
        if 'payload' in http_packet:
            new_file_location = http_packet['payload']
            filelocation_list.update(new_file_location) # {"machine1.log":[1,2,3]} -> {"machine1.log":[1,2,3], "machine2.log":[]}
            logger.info(f"File location list update {str(new_file_location)}")
        else:
            del_sdfs = http_packet['sdfs_filename']
            del filelocation_list[del_sdfs]
            logger.info(f"Delete {str(del_sdfs)} Success")
    except Exception as e:
        logger.error(f'Error {str(e)}')
    

def handle_request(clientsocket, ip):
    """
    These function handles are requests that are from the leader.
    """
    query = clientsocket.recv(4096)
    
    http_packet = query.decode(msg_format)
    http_packet = json.loads(http_packet)

    logger.info(f"Received Task {str(http_packet)}")
    # each of request_type might send x
    if http_packet['request_type'] == 'put':
        put_file(http_packet)
    elif http_packet['request_type'] == 'get':
        get_file(http_packet)
    elif http_packet['request_type'] == 'delete':
        delete_file(http_packet)
    elif http_packet['request_type'] == 'update':
        update_file(http_packet)
    elif http_packet['request_type'] == 'finish_ack':
        # if http_packet['output_file'] is not None:
        #     print(f"Output file saved at {http_packet['output_file']} ")
        # if http_packet['task_id'] is not None:
        task_id = http_packet['task_id']         
        print(f"Task {task_id} finished from all servers")
    elif http_packet['request_type'] == 'display':
        sdfs_output = http_packet['output_file']
        print(f"Output is saved to: {sdfs_output}")
    # elif http_packet['request_type'] == 'maple':
    #     maple_ex = http_packet['maple_exe']
    #     num_map = http_packet['num_maples']
    #     iterm_prefix = http_packet['interm']
    #     sdfs_dir = http_packet['source']
    #     # source = http_packet['request_source']
    #     maple_entrypoint(maple_ex, num_map, iterm_prefix, sdfs_dir)   

def partition_csv(intermediate, input_file, n, dest_dir):
    n = int(n)
    logger.info(f"Starting CSV partitioning for {input_file}")

    with open(input_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        rows = list(reader)

    num_rows = len(rows)
    n = min(n, num_rows)
    rows_per_partition = (num_rows + n - 1) // n

    partitioned_files = []  # List to store the names of partitioned files

    for i in range(n):
        output_file_path = os.path.join(dest_dir, f"{intermediate}-p{i}.csv")
        with open(output_file_path, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            start = i * rows_per_partition
            end = min(start + rows_per_partition, num_rows)
            writer.writerows(rows[start:end])
        logger.info(f"Created partition {output_file_path}")
        partitioned_files.append(output_file_path)  # Add the file name to the list

    return partitioned_files  # Return the list of partitioned file names

# put /home/jsku2/cs425_mp4/data/songs.csv songs.csv

# maple /home/jsku2/cs425_mp4/data/songs-exe 2 song songs.csv

def maple_entrypoint(m_exe, num_map, sdfs_prefix, sdfs_dir, interconn=None):
    global filelocation_list
    logger.info("Maple entrypoint called")
    local_input_file = f'/home/jsku2/MP3_LOCAL/{sdfs_dir}'

    logger.info(f"Requesting file {sdfs_dir} from leader")

    http_packet = {}
    http_packet['local_filename'] = local_input_file
    http_packet['sdfs_filename'] = sdfs_dir
    http_packet['request_source'] = host_domain_name
    http_packet['request_type'] = 'get'

    loc = filelocation_list[sdfs_dir]
    logger.info(f"")
    send(http_packet, 'get', False, loc)

    start_time = time.time()
    while not os.path.exists(local_input_file):
        logger.info(f"Waiting for file {local_input_file} to be available")
        time.sleep(0.5)  # Wait for 0.5 seconds before checking again
        if time.time() - start_time > 30:  # Timeout after 30 seconds
            logger.error(f"Timeout: File {local_input_file} not found.")
            return

    logger.info(f"Partitioning file {local_input_file}")
    sharded_list = partition_csv(sdfs_prefix, local_input_file, num_map, '/home/jsku2/shardedLeader')
    maple_scheduler(m_exe, sharded_list, sdfs_prefix, interconn)

    logger.info("Maple entrypoint finished")
    return True

def csv_to_list(csv_string, source):
    # Split the string by commas, convert each element to an integer,
    # and create a dictionary for each item
    return [{'source': source, 'file': file} for file in csv_string[:-1].split('\n')]

def maple_scheduler(m_exec, shardedList, interm, interconn):
    global node_task_map
    global maple_out_list
    available_nodes = list(fail_detector.membership_list.keys())

    logger.info(f"shardedList: {shardedList}")
    logger.info(f"Available nodes: {available_nodes}")

    for shard_idx in range(len(shardedList)):
        node = available_nodes[shard_idx % len(available_nodes)]
        exec_filename = os.path.basename(m_exec)
        shard_filename = os.path.basename(shardedList[shard_idx])

        # Prepare task info
        task_info = {
            'node': node,
            'exec_file': exec_filename,
            'shard_file_path': shardedList[shard_idx],
            'interm_prefix': interm,
            # pending | success
            'status': "pending"
        }

        # Append task to node_task_map for rescheduling if needed
        if node in node_task_map:
            node_task_map[node].append(task_info)
        else:
            node_task_map[node] = [task_info]

        logger.info(f"exec_filename: {exec_filename}")
        logger.info(f"shard_filename: {shard_filename}")

        cmd_shard = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {shardedList[shard_idx]} jsku2@{node}:/home/jsku2/shardedWorker/{shard_filename}'
        cmd_exec = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/cs425_mp4/data/{m_exec} jsku2@{node}:/home/jsku2/shardedWorker/{exec_filename}'
        
        try:
            logger.info(f"Executing command: {cmd_shard}")
            result1 = subprocess.check_output(cmd_shard, shell=True, stderr=subprocess.STDOUT)
            logger.info(f"Command output: {result1.decode('utf-8')}")

            logger.info(f"Executing command: {cmd_exec}")
            result2 = subprocess.check_output(cmd_exec, shell=True, stderr=subprocess.STDOUT)
            logger.info(f"Command output: {result2.decode('utf-8')}")

            logger.info(f"Connecting to node: {node}")
            conn = rpyc.connect(node, 18812)  # Replace 18812 with the port number used in your RPyC service
            remote_command = f"cd /home/jsku2/shardedWorker && ./{m_exec} {interm} {interconn}"
            command_output = conn.root.exposed_execute_command(remote_command)
            
            maple_out_list = maple_out_list + csv_to_list(command_output, node)

            logger.info(f"Executed command on {node}: {remote_command}")
            logger.info(f"Remote command output: {command_output}")
            conn.close()

        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing subprocess command: {e.cmd}")
            logger.error(f"Exit status: {e.returncode}")
            logger.error(f"Output: {e.output.decode('utf-8')}")

        except Exception as e:
            logger.error(f"General error: {str(e)}")

    logger.info(f"MAPLE-FILES HERE: {maple_out_list}")
    return True

def reschedule_task(exec, host, interm, shard_file, type, regex=None):
    # 1. Leader put shard_file to host
    # 2. Set up RPC 
    # 3. Run executable
    global maple_out_list
    logger.info(f"reschedule_task is called... Rescheduling {shard_file}... on {host}")
    exec_filename = os.path.basename(exec)
    shard_filename = os.path.basename(shard_file)

    cmd_shard = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {shard_file} jsku2@{host}:/home/jsku2/shardedWorker/{shard_filename}'
    cmd_exec = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/cs425_mp4/data/{exec} jsku2@{host}:/home/jsku2/shardedWorker/{exec_filename}'
        
    if type == "map": 
        try:
            logger.info(f"Executing command 1: {cmd_shard}")
            result1 = subprocess.check_output(cmd_shard, shell=True, stderr=subprocess.STDOUT)
            logger.info(f"Command output: {result1.decode('utf-8')}")

            logger.info(f"Executing command 2: {cmd_exec}")
            result2 = subprocess.check_output(cmd_exec, shell=True, stderr=subprocess.STDOUT)
            logger.info(f"Command output: {result2.decode('utf-8')}")

            logger.info(f"Connecting to node: {host}")
            conn = rpyc.connect(host, 18812)  # Replace 18812 with the port number used in your RPyC service
            remote_command = f"cd /home/jsku2/shardedWorker && ./{exec} {interm}"
            command_output = conn.root.exposed_execute_command(remote_command)
            
            maple_out_list = maple_out_list + csv_to_list(command_output, host)

            logger.info(f"Executed command on {host}: {remote_command}")
            logger.info(f"Remote command output: {command_output}")
            conn.close()
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing subprocess command: {e.cmd}")
            logger.error(f"Exit status: {e.returncode}")
            logger.error(f"Output: {e.output.decode('utf-8')}")

        except Exception as e:
            logger.error(f"General error: {str(e)}")

    elif type == "juice":
        juice_exe_scp = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/cs425_mp4/data/{exec} jsku2@{host}:/home/jsku2/pre-juice/{exec}'
        juice_schedule = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {shard_file} jsku2@{host}:/home/jsku2/pre-juice/{shard_file}'
            
        try:
            logger.info(f"Executing SCP command 11: {juice_schedule}")
            conn.root.exposed_execute_command(juice_schedule)

            result = subprocess.check_output(juice_exe_scp, shell=True)
            # connect to juice wokrere
            logger.info(f"Connecting to node 22: {host} for file transfer")
            conn = rpyc.connect(host, 18812)  # Replace 18812 with the port number used in your RPyC service

            if regex is None:
                remote_command = f"cd /home/jsku2/pre-juice && ./{exec} {interm}"
                logger.info(f"Executing remote command: {remote_command}")
                command_output = conn.root.exposed_execute_command(remote_command)
                # Retrieve file objects
                logger.info(f"Remote command output: {command_output}")
            else:
                remote_command = f"cd /home/jsku2/pre-juice && ./{exec} {interm} {regex}"
                logger.info(f"Executing remote command: {remote_command}")
                command_output = conn.root.exposed_execute_command(remote_command)
                # Retrieve file objects
                logger.info(f"Remote command output: {command_output}")

            # Convert file object to a list
            juice_out_list = juice_out_list + csv_to_list(command_output, host)

            # Collect final files from post-juice and merge them from the picked machine -> leader

            pattern = f"{interm}.*\\.csv"  
            matched_files = []

            # List files in the directory and filter with regex
            list_files_command = "ls /home/jsku2/juice/"
            logger.info(f"Listing files in directory with command: {list_files_command}")
            files_list = conn.root.exposed_execute_command(list_files_command).split('\n')
            logger.info(f"file list in juice dir: {files_list}")

            for file in files_list:
                if re.match(pattern, file):
                    matched_files.append(file)

            logger.info(f"matched file list: {matched_files}")
            for file in matched_files:
                scp_command = f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/juice/{file} jsku2@{host_domain_name}:/home/jsku2/post-juice/{file}"
                logger.info(f"Executing SCP command for file: {scp_command}")
                conn.root.exposed_execute_command(scp_command)

        except Exception as e:
            logger.error(f"Error during juice collection: {str(e)}")
    else:
        logger.info("wrong type to reschedule")
        
    return True


def juice_entrypoint(j_exe, num_j, interm_p, sdfs_dir, delete, partition, requester=None, regex=None):
    global juice_out_list
    num_j = int(num_j)
    # 1. pick num_j machines
    available_nodes = list(fail_detector.membership_list.keys())
    picked_machines = random.sample(available_nodes, min(num_j, len(available_nodes)))
    partition_list = []

    # 2. parition maple_out_list into num_j
    if partition == "hash":
        logger.info("hash partition")
        partition_list = hash_partition(num_j)
    elif partition == "range":
        logger.info("range partition")
        partition_list = range_partition(num_j)
    else:
        logger.info("partition method not supported")
    
    logger.info(f"Partition list: {partition_list}")

    for i in range(len(picked_machines)):
        # [{'source': 'fa23-cs425-7302.cs.illinois.edu', 'file': '/home/jsku2/maple/song-p0-1983.csv'}, 
        # {'source': 'fa23-cs425-7303.cs.illinois.edu', 'file': '/home/jsku2/maple/song-p0-1983.csv'}, 
        # {'source': 'fa23-cs425-7306.cs.illinois.edu', 'file': '/home/jsku2/maple/song-p0-1983.csv'}]
        juice_exe_scp = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/cs425_mp4/data/{j_exe} jsku2@{picked_machines[i]}:/home/jsku2/pre-juice/{j_exe}'
        for file_obj in partition_list[i]:
            ## TODO: needs to decide on directory path
            shard_filename = os.path.basename(file_obj["file"])
            juice_schedule = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {file_obj["file"]} jsku2@{picked_machines[i]}:/home/jsku2/pre-juice/{shard_filename}'
            juice_task_info = {
                'node': picked_machines[i],
                'exec_file': j_exe,
                'file_path': file_obj["file"],
                'interm_prefix': interm_p,
                'status': 'pending'  

            }
            juice_out_list.append(juice_task_info)

            for task in node_task_map.get(file_obj['source'], []):
                if task['shard_file_path'] == file_obj["file"]:
                    task['status'] = 'success'
                    logger.info(f"Updated map task status to 'success' for {shard_filename} on node {file_obj['source']}")
                   
            try:
                # Put files from maple to juice worker
                # Connect to the machine that has the maple output file
                logger.info(f"Connecting to node: {file_obj['source']} for file transfer")
                conn = rpyc.connect(file_obj['source'], 18812)  # Replace 18812 with the port number used in your RPyC service

                # Put juice executable from maple to juice worker
                logger.info(f"Executing SCP command: {juice_schedule}")
                conn.root.exposed_execute_command(juice_schedule)

            except Exception as e:
                logger.error(f"Error: {str(e)}")

        # Execute juice-exe command
        try:
            result = subprocess.check_output(juice_exe_scp, shell=True)
            # connect to juice wokrere
            logger.info(f"Connecting to node: {picked_machines[i]} for file transfer")
            conn = rpyc.connect(picked_machines[i], 18812)  # Replace 18812 with the port number used in your RPyC service

            if regex is None:
                remote_command = f"cd /home/jsku2/pre-juice && ./{j_exe} {interm_p}"
                logger.info(f"Executing remote command: {remote_command}")
                command_output = conn.root.exposed_execute_command(remote_command)
                # Retrieve file objects
                logger.info(f"Remote command output: {command_output}")
            else:
                remote_command = f"cd /home/jsku2/pre-juice && ./{j_exe} {interm_p} {regex}"
                logger.info(f"Executing remote command: {remote_command}")
                command_output = conn.root.exposed_execute_command(remote_command)
                # Retrieve file objects
                logger.info(f"Remote command output: {command_output}")

            # Convert file object to a list
            juice_out_list = juice_out_list + csv_to_list(command_output, picked_machines[i])

            pattern = f"{interm_p}.*\\.csv"  
            matched_files = []

            # List files in the directory and filter with regex
            list_files_command = "ls /home/jsku2/juice/"
            logger.info(f"Listing files in directory with command: {list_files_command}")
            files_list = conn.root.exposed_execute_command(list_files_command).split('\n')
            logger.info(f"file list in juice dir: {files_list}")

            for file in files_list:
                if re.match(pattern, file):
                    matched_files.append(file)

            logger.info(f"matched file list: {matched_files}")
            for file in matched_files:
                scp_command = f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/jsku2/juice/{file} jsku2@{host_domain_name}:/home/jsku2/post-juice/{file}"
                logger.info(f"Executing SCP command for file: {scp_command}")
                conn.root.exposed_execute_command(scp_command)

        except Exception as e:
            logger.error(f"Error during juice collection: {str(e)}")
        
    def conglomerate(prefix, output_filename, dir):
        # List to hold all the rows from the files
        all_rows = []

        logger.info(f"Searching in directory: {dir} for files starting with prefix: {prefix}")

        # Iterate through each file in the directory
        for filename in os.listdir(dir):
            # Check if the file matches the prefix and is a CSV file
            if filename.startswith(prefix) and filename.endswith(".csv"):
                logger.info(f"Found file: {filename}")

                # Construct full path to the file
                file_path = os.path.join(dir, filename)
                logger.info(f"Reading from file: {file_path}")

                # Open the file and read its contents
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        all_rows.append(row)
                        logger.info(f"Read row: {row}")

        # Write the combined content to a new file
        output_file_path = os.path.join("/home/jsku2/MP4_FILE/", f"{output_filename}")
        logger.info(f"Writing to output file: {output_file_path}")

        with open(output_file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            for row in all_rows:
                writer.writerow(row)
                logger.info(f"Wrote row: {row}")

        return output_file_path

    # Display output file path on requester
    outputLocation = conglomerate(interm_p, sdfs_dir, "/home/jsku2/post-juice/")
    send2Leader('put', sdfs_dir, outputLocation)

    juice_ack_packet = {}
    juice_ack_packet['request_type'] = 'display'
    juice_ack_packet['output_file'] = sdfs_dir
    send(juice_ack_packet, 'display', False, [requester])

    if int(delete) == 1:
        available_nodes = list(fail_detector.membership_list.keys())
        for n in available_nodes:
            logger.info(f"Connecting to node to clear: {n} for file transfer")
            conn = rpyc.connect(n, 18812)  # Replace 18812 with the port number used in your RPyC service
            clear_cmd = f"cd /home/jsku2/cs425_mp4/file_server && ./clear_dir.sh"
            # Put juice executable from maple to juice worker
            logger.info(f"Executing SCP command: {clear_cmd}")
            conn.root.exposed_execute_command(clear_cmd)

    logger.info("Juice entrypoint finished")

    return True


def SELECT_entrypoint(data, reg, requester):
    # 1. find availbe num of hosts
    available_nodes = list(fail_detector.membership_list.keys())
    num_m = len(available_nodes)
    ### SQL needs it's own executable that can take regex ###
    
    output_file_path = ""
    maple_done = maple_entrypoint("sql-maple", num_m, "select", data)
    if maple_done:
        available_nodes_j = list(fail_detector.membership_list.keys())
        num_j = len(available_nodes)
        output_file_path = "sql-juice-output.csv"
        juice_entrypoint("sql-juice", num_j, "select", output_file_path, 1, "hash", requester, reg)

    return True


def extract_key_from_filename(filename):
    # Extract the key (year) from the filename
    # Assuming the filename format is 'song-p0-<year>.csv'
    try:
        return filename.split('--')[-1].split('.')[0]  # Extracts '1983' from 'song-p0-1983.csv'
    except IndexError:
        return None  # or some default value

def hash_partition(num):
    # Initialize a list of lists for partitions
    partitions = [[] for _ in range(num)]

    for item in maple_out_list:
        # Extract the key from the file name
        key = extract_key_from_filename(item['file'])

        # Hash the key and use modulo to find the index
        if key is not None:
            index = hash(key) % num
            partitions[index].append(item)
        else:
            # Handle cases where the key could not be extracted
            # For example, you might choose to ignore such items or log an error
            logger.info("key not found")
    return partitions

def range_partition(num):
    partitions = [[] for _ in range(num)]

    key_list = []
    for item in maple_out_list:
        key = extract_key_from_filename(item['file'])
        if key is not None:
            key_list.append(key)

    if not key_list:
        logger.info("No valid keys are found")
        return partitions  # Return empty partitions if no valid keys are found

    key_list.sort()  # Sort the keys lexicographically
    partition_size = len(key_list) // num  # Determine the size of each partition
    extra = len(key_list) % num  # Additional items to distribute evenly

    partition_ranges = []
    start = 0
    for i in range(num):
        end = start + partition_size + (1 if i < extra else 0)
        partition_ranges.append((key_list[start], key_list[min(end, len(key_list) - 1)]))
        start = end

    for item in maple_out_list:
        key = extract_key_from_filename(item['file'])
        if key is not None:
            for idx, (start_key, end_key) in enumerate(partition_ranges):
                if start_key <= key <= end_key:
                    partitions[idx].append(item)
                    break

    return partitions

def select_new_host(exclude_host):
    available_nodes = [node for node in list(fail_detector.membership_list.keys()) if node != exclude_host]
    return random.choice(available_nodes) if available_nodes else None

# Only Leader should run rescheduler
def rescheduler(failed_host):
    global maple_out_list, juice_out_list, node_task_map, node_task_juice
    
    logger.info(f"Rescheduling tasks for failed host: {failed_host}")

    # Reschedule Maple tasks
    if failed_host in node_task_map:
        maple_tasks = node_task_map.pop(failed_host)
        logger.info(f"maple_tasks: {maple_tasks}")
        for task in maple_tasks:
            if task['status'] == 'pending':
                # Assign to a new machine
                new_host = select_new_host(failed_host)
                logger.info(f"New host for maple: {new_host}")

                # Update task info
                task['node'] = new_host
                task['status'] = 'pending'  # Reset status to pending

                if new_host in node_task_map:
                    node_task_map[new_host].append(task)
                else:
                    node_task_map[new_host] = [task]

                reschedule_task(task['exec_file'], new_host, task['interm_prefix'], task['shard_file_path'], "map")
            else:
                logger.info(f"Skipping rescheduling for map completed task: {task['shard_file_path']}")
    # Reschedule Juice task
    if failed_host in node_task_juice:
        juice_tasks = node_task_juice.pop(failed_host)
        for task in juice_tasks:
            if task['status'] == 'pending':
                # Assign to a new machine
                new_host = select_new_host(failed_host)
                logger.info(f"New host for juice: {new_host}")
                if new_host in node_task_map:
                    node_task_juice[new_host].append(task)
                else:
                    node_task_juice[new_host] = [task]

                # Update task info
                task['node'] = new_host
                task['status'] = 'pending'  # Reset status to pending
                
                if new_host in node_task_juice:
                    node_task_juice[new_host].append(task)
                else:
                    node_task_juice[new_host] = [task]
                reschedule_task(task['exec_file'], new_host, task['interm_prefix'], task['shard_file_path'], "juice")
            else:
                logger.info(f"Skipping rescheduling juice for completed task: {task['shard_file_path']}")
    return True

def reciever():
    logger.info("listening and dealing with requests")
    thread_pool = {}
    file_sockets['reciever'].listen()                 


    while True:
        '''
        listening query (accept each connection), trigger working thread per query
        '''
        clientsocket, clientip = file_sockets['reciever'].accept()     
        '''
        multithreading programming: label each thread with query's timestamp and client ip
        end_tasks: record task_id of finished threads
        thread_pool: dictionary, key: task_id, value:working thread
        '''

        task_id = str(clientip)+'_'+str(datetime.datetime.now())
        end_tasks = []
        thread_pool[task_id] = threading.Thread(target=handle_request, args=[clientsocket, clientip])
        thread_pool[task_id].start()
        '''
        multithreading programming: join finished threads. since dictionary can't be modified during iteration
        , add the finished tasks into end_tasks, then join those thread with end_tasks
        '''
        for task in thread_pool:
            if not thread_pool[task].is_alive():
                end_tasks.append(task)
        for task in end_tasks:
            thread_pool[task_id].join()
            del thread_pool[task]
            logger.info("Finish task:[{}]".format(task))

def send2Member():
    """
    This function is from the leader to prepare the http_packet to the members
    """
    global leader_queue

    while True:

        remove_task = []
        waiting_write = defaultdict(lambda: False) 
        waiting_read = defaultdict(lambda: False) 
        for i in range(len(leader_queue)):

            http_packet = leader_queue[i]

            location_packet = {}
            #schedule_counter = {'sdfsfilename':[R_count, W_count, R_pre, W_pre]} 
            #WRITE
            if http_packet['request_type'] == 'put': 
                if schedule_counter[http_packet['sdfs_filename']][0]>0 or schedule_counter[http_packet['sdfs_filename']][1]>0 \
                    or schedule_counter[http_packet['sdfs_filename']][3]>=4:
                    waiting_write[http_packet['sdfs_filename']] = True
                    continue
                else:
                    waiting_read[http_packet['sdfs_filename']] = waiting_read[http_packet['sdfs_filename']]
                    schedule_counter[http_packet['sdfs_filename']][3] = 1
                    schedule_counter[http_packet['sdfs_filename']][2] = 0
                    schedule_counter[http_packet['sdfs_filename']][1] = 1
                remove_task.append(i)

                # select to do job
                members = list(fail_detector.membership_list.keys())
                print("Members: ",members)
                members.remove(host_domain_name)

                sdfs_filename = http_packet['sdfs_filename']
                if sdfs_filename in filelocation_list:
                    replica_ips = filelocation_list[sdfs_filename]
                else:
                    if len(members) >= 4:
                        replica_ips = random.sample(members, 4)
                    else:
                        replica_ips = random.sample(members, len(members))

                print("Replica_ips ", replica_ips)
                filelocation_list[sdfs_filename] = replica_ips
                # add counter after a job is exectued
                send(http_packet, 'put', False, replica_ips)
                location_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                location_packet['request_type'] = 'update'
                location_packet['sdfs_filename'] = sdfs_filename
                location_packet['payload'] = {sdfs_filename:replica_ips}
                send(location_packet, 'update', False)

            #READ
            elif http_packet['request_type'] == 'get':

                if schedule_counter[http_packet['sdfs_filename']][0]>1 or schedule_counter[http_packet['sdfs_filename']][1]>0 \
                    or schedule_counter[http_packet['sdfs_filename']][2]>=4:
                    waiting_read[http_packet['sdfs_filename']] = True
                    continue
                else:
                    waiting_write[http_packet['sdfs_filename']] = waiting_write[http_packet['sdfs_filename']]
                    schedule_counter[http_packet['sdfs_filename']][3] = 0
                    schedule_counter[http_packet['sdfs_filename']][2] += 1
                    schedule_counter[http_packet['sdfs_filename']][0] += 1
                remove_task.append(i)
                
                # this should be chnaged, we need to get from a server and delete all file on servers
                # should also consider the mechnism of maintaining file table
                
                sdfs_filename = http_packet['sdfs_filename']
                logger.info(filelocation_list)
                logger.info(sdfs_filename)
                loc = filelocation_list[sdfs_filename]
                send(http_packet, 'get', False, loc)

            elif http_packet['request_type'] == 'delete':
                # Re-schedule Counter
                if schedule_counter[http_packet['sdfs_filename']][0]>0 or schedule_counter[http_packet['sdfs_filename']][1]>0 \
                    or schedule_counter[http_packet['sdfs_filename']][3]>=4:
                    waiting_write[http_packet['sdfs_filename']] = True
                    continue
                else:
                    waiting_read[http_packet['sdfs_filename']] = waiting_read[http_packet['sdfs_filename']]
                    schedule_counter[http_packet['sdfs_filename']][3] = 1
                    schedule_counter[http_packet['sdfs_filename']][2] = 0
                    schedule_counter[http_packet['sdfs_filename']][1] = 1
                remove_task.append(i)

                # Do Delete task here
                try:
                    sdfs_filename = http_packet['sdfs_filename']
                    send(http_packet, request_type, False, filelocation_list[sdfs_filename])
                    location_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                    location_packet['request_type'] = 'update'
                    location_packet['sdfs_filename'] = sdfs_filename
                    send(location_packet, 'update', False)
                except Exception as e:
                    logger.error(f"Error {str(e)}")

            elif http_packet['request_type'] =='put_ack':
                global put_ack
                source = http_packet['request_source']
                sdfs_filename = http_packet['sdfs_filename']
                replica_ip = http_packet['replica_ip']
                put_ack[sdfs_filename].append(replica_ip)
                check_list = set(list(fail_detector.membership_list.keys())) & set(filelocation_list[sdfs_filename])
                remove_task.append(i)
                if sorted(list(check_list)) == sorted(put_ack[sdfs_filename]) or len(put_ack[sdfs_filename])>=len(check_list): 
                    print(f"SEND FINISH ACK TO THE USER REQUEST SOURCE at {str(replica_ip)}, check_list is {str(check_list)}, put_ack is {str(sorted(put_ack[sdfs_filename]))}")
                    put_ack[sdfs_filename] = []
                    schedule_counter[sdfs_filename][1] = 0
                    ack_packet = {}
                    ack_packet['request_type'] = 'finish_ack'
                    ack_packet['task_id'] = http_packet['task_id']
                    send(ack_packet, 'finish_ack', False, [source])
                    # remove_task.append(i)
            
            elif http_packet['request_type'] =='delete_ack':
                global delete_ack
                source = http_packet['request_source']
                sdfs_filename = http_packet['sdfs_filename']
                replica_ip = http_packet['replica_ip']
                delete_ack[sdfs_filename].append(replica_ip)
                check_list = set(fail_detector.membership_list.keys()) & set(filelocation_list[sdfs_filename])
                remove_task.append(i)
                if sorted(list(check_list)) == sorted(delete_ack[sdfs_filename]) or len(delete_ack[sdfs_filename])>=len(check_list): 
                    delete_ack[sdfs_filename] = []
                    schedule_counter[sdfs_filename][1] = 0
                    # after receive all acks, delete from file location list
                    del filelocation_list[sdfs_filename]
                    ack_packet = {}
                    ack_packet['request_type'] = 'finish_ack'
                    ack_packet['task_id'] = http_packet['task_id']
                    send(ack_packet, 'finish_ack', False, [source])
                    # remove_task.append(i)
            
            elif http_packet['request_type'] =='get_ack':
                sdfs_filename = http_packet['sdfs_filename']
                source = http_packet['request_source']
                schedule_counter[sdfs_filename][0] -= 1
                ack_packet = {}
                ack_packet['request_type'] = 'finish_ack'
                ack_packet['task_id'] = http_packet['task_id']
                send(ack_packet, 'finish_ack', False, [source])
                remove_task.append(i)
            elif http_packet['request_type'] =='maple':
                maple_ex = http_packet['maple_exe']
                num_map = http_packet['num_maples']
                iterm_prefix = http_packet['interm']
                sdfs_dir = http_packet['source']
                internconn = http_packet['intercon']
                remove_task.append(i)
                maple_entrypoint(maple_ex, num_map, iterm_prefix, sdfs_dir, internconn)
            elif http_packet['request_type'] =='juice':
                juice_ex = http_packet['juice_exe']
                num_jui = http_packet['num_juices']
                interm_predix_jui = http_packet['sdfs_interm']
                sdfs_dir_jui = http_packet['sdfs_dir']
                delete = http_packet['delete']
                partition = http_packet['partition']
                requester = http_packet['request_source']
                remove_task.append(i)
                juice_entrypoint(juice_ex, num_jui, interm_predix_jui, sdfs_dir_jui, delete, partition, requester)
            elif http_packet['request_type'] =='SELECT':
                dataset = http_packet['dataset'] 
                regex = http_packet['regex']
                requester = http_packet['request_source']
                # task = http_packet['task_id']
                remove_task.append(i)
                SELECT_entrypoint(dataset, regex, requester)
            else:
               print(f"INVALID request_type {request_type}")

        # handle logic to avoid continous same operation and starvation
        for file_name, check in waiting_read.items():
            if not check:
                schedule_counter[file_name][3] = 0
        for file_name, check in waiting_write.items():
            if not check:
                schedule_counter[file_name][2] = 0

        # remove tasks that are done from job queue
        for task in remove_task[::-1]:
            del leader_queue[task]
    

def rereplicate():
    """
        This function always detects if there are failures and proceed with rereplication
    """
    while True:
        while len(fail_detector.failure_queue) > 0:
            domain_name = fail_detector.failure_queue.popleft()
            # Failure occured! fa23-cs425-7307.cs.illinois.edu
            print(f"Failure occured! {str(domain_name)}")
            for sdfs_filename, ips in filelocation_list.items():
                if domain_name in ips:
                    # update filelocation_list for all
                    filelocation_list[sdfs_filename].remove(domain_name)
                    location_packet = {}
                    location_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                    location_packet['request_type'] = 'update'
                    location_packet['sdfs_filename'] = sdfs_filename
                    
                    send_fail = True
                    while send_fail: 
                    # create one new replica
                        mb_list = set(fail_detector.membership_list)
                        replica_source = random.choice(filelocation_list[sdfs_filename])
                        dest = random.choice(list(mb_list - set(filelocation_list[sdfs_filename])))

                        # If there is enough space to put replica
                        if dest:
                            # Add new replica destinaion if availble
                            filelocation_list[sdfs_filename].append(dest)
                            http_packet = {}
                            http_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                            http_packet['sdfs_filename'] = sdfs_filename
                            http_packet['local_filename'] = '/home/jsku2/MP3_FILE/' + sdfs_filename
                            http_packet['request_type'] = "put"
                            # If request_type == 'put': user input put localfilename, sdfs_filename, source == user host_domain, replica_ips == destination
                            http_packet['request_source'] = replica_source
                            send_fail = not (send(http_packet, 'rereplicate', False, [dest]))
                            if send_fail:
                                filelocation_list[sdfs_filename].remove(dest)
                        else:
                            send_fail = False

                    rescheduler(domain_name)
                    # Send the location packet in 2 cases: 1. there is new replica destination 2. No new deplica destination, just remove it.
                    location_packet['payload'] = {sdfs_filename:filelocation_list[sdfs_filename]}
                    send(location_packet, 'update', False)
                    

def intro_new_join():
    while True:
        while len(fail_detector.filelocation_intro_queue) > 0:
            domain_name = fail_detector.filelocation_intro_queue.popleft()
            location_packet = {}
            location_packet['task_id'] = 'newjoin_' + host_domain_name + '_'+str(datetime.datetime.now())  
            location_packet['request_type'] = 'update'
            location_packet['payload'] = filelocation_list
            send(location_packet, 'update', False)

def leader_main():
    global leader_queue

    # Keep checking if self is leader
    while True:
        if len(fail_detector.membership_list.keys())>1:
            min_now = min(fail_detector.membership_list.keys())
            if min_now == host_domain_name:
                break
    
    rereplicate_thread = threading.Thread(target = rereplicate)
    rereplicate_thread.start()

    intro_new_join_thread = threading.Thread(target = intro_new_join)
    intro_new_join_thread.start()

    file_sockets['leader'].listen()
    while True:
        clientsocket, clientip = file_sockets['leader'].accept()
        query = clientsocket.recv(1024)
        http_packet = query.decode(msg_format)
        http_packet = json.loads(http_packet)
        logger.info(f"Receive {http_packet['request_type']} from {http_packet['request_source']}")
        leader_queue.append(http_packet)
    

def send2Leader(request_type, sdfs_filename, local_filename = None, host_domain_name = host_domain_name):
    """
    This function is to handle user inputs and prepare packet to send to leader.
    """

    http_packet = {}
    http_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())
    print(f"Task {http_packet['task_id']} starts! ")  
    http_packet['sdfs_filename'] = sdfs_filename
    http_packet['local_filename'] = local_filename
    http_packet['request_type'] = request_type
    http_packet['request_source'] = host_domain_name

    # use socket['result_port] to get result
    if request_type in ['put', 'get', 'delete']:
        send(http_packet, request_type, True)

    else:
        print(f"INVALID request_type {request_type}")

def clean_local_sdfs_dir():
    try:
        # init sdfs
        cmd = 'rm -rf /home/jsku2/MP3_FILE'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully remove sdfs directory")
        cmd = 'mkdir -p /home/jsku2/MP3_FILE'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully create sdfs directory")
        # init local to get files from sdfs
        cmd = 'rm -rf /home/jsku2/MP3_LOCAL'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully remove local directory")
        cmd = 'mkdir -p /home/jsku2/MP3_LOCAL'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully create local directory")
        # shardedLeader
        cmd = 'rm -rf /home/jsku2/shardedLeader'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully remove shardedLeader directory")
        cmd = 'mkdir -p /home/jsku2/shardedLeader'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully create shardedLeader directory")
        # shardedWorker
        cmd = 'rm -rf /home/jsku2/shardedWorker'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully remove shardedWorker directory")
        cmd = 'mkdir -p /home/jsku2/shardedWorker'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully create shardedWorker directory")
        # maple
        cmd = 'rm -rf /home/jsku2/maple'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully remove maple directory")
        cmd = 'mkdir -p /home/jsku2/maple'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully create maple directory")
        # pre-juice
        cmd = 'rm -rf /home/jsku2/pre-juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully removed pre-juice directory")
        cmd = 'mkdir -p /home/jsku2/pre-juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully created pre-juice directory")
        # juice
        cmd = 'rm -rf /home/jsku2/juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully removed juice directory")
        cmd = 'mkdir -p /home/jsku2/juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully created juice directory")
        # post-juice
        cmd = 'rm -rf /home/jsku2/post-juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully removed post-juice directory")
        cmd = 'mkdir -p /home/jsku2/post-juice'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully created post-juice directory")
        # MP4_FILE
        cmd = 'rm -rf /home/jsku2/MP4_FILE'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully removed MP4_FILE directory")
        cmd = 'mkdir -p /home/jsku2/MP4_FILE'
        result = subprocess.check_output(cmd, shell=True)
        logger.info("successfully created MP4_FILE directory")

    except Exception as e:
        logger.error(f"init local/sdfs dir error: {str(e)}")
    
# watches for container queue
def process_queue():
    while True:
        if not container_queue.empty():
            item = container_queue.get()
            # Feed item to Maple_exe
            print(f"Processing item: {item}")
            container_queue.task_done()

        else:
            time.sleep(0.1)       

if __name__ == "__main__":

    clean_local_sdfs_dir()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
    hostip = socket.gethostname() 
    hostport = file_reciever_port               

    sock.bind((hostip, hostport))
    file_sockets['reciever'] = sock

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
    hostip = socket.gethostname() 
    hostport = file_leader_port               

    sock.bind((hostip, hostport))
    file_sockets['leader'] = sock

    server_thread = threading.Thread(target=fail_detector.start_gossiping)
    server_thread.start()

    leader_thread = threading.Thread(target=leader_main)
    leader_thread.start()

    listening_thread = threading.Thread(target = reciever)
    listening_thread.start()

    leader_function_thread = threading.Thread(target=send2Member)
    leader_function_thread.start()

    queue_thread = threading.Thread(target=process_queue)
    queue_thread.start()

    rpyc_thread = threading.Thread(target=rpyc_server_thread)
    rpyc_thread.start()

    while True:
        user_input = input("Please Enter message for SDFS: ")
        request_type = user_input.split(' ')[0]
        # SPECIFY LEADER TO MINIMUM ID
        try: 
            
            if request_type.lower() == "put": # host start to leave the gossiping group
                local_filename, sdfs_filename = user_input.split(' ')[1], user_input.split(' ')[2]
                send2Leader(request_type, sdfs_filename, local_filename)

            elif request_type.lower() == 'get':
                local_filename, sdfs_filename = user_input.split(' ')[1], user_input.split(' ')[2]
                send2Leader(request_type, sdfs_filename, local_filename)
            
            elif request_type.lower() == 'delete':
                sdfs_filename = user_input.split(' ')[1]
                send2Leader(request_type, sdfs_filename)

            elif request_type.lower() == 'ls':
                sdfs_filename = user_input.split(' ')[1]
                try:
                    print(f"Machines that store {str(sdfs_filename)} is : {str(filelocation_list[sdfs_filename])}")
                except:
                    print(f"Machines that store {str(sdfs_filename)} is : None")
            
            elif user_input.lower() == 'store':
                files = []
                for k, v in filelocation_list.items():
                    if host_domain_name in v:
                        files.append(k)
                print(f"Files store on {machine_id} is {str(files)}")
            
            elif request_type.lower() == 'multiread': # multiread sdfs_filename 2 7
                sdfs_filename, m = user_input.split(' ')[1], user_input.split(' ')[2]
                local_filename = f'/home/jsku2/MP3_LOCAL/{sdfs_filename}'
                mems = list(fail_detector.membership_list.keys())
                random_vms = random.sample(mems, k = int(m))
                print(f"Target VMS: {str(random_vms)}")
                for dest_domain_name in random_vms:
                    send2Leader('get', sdfs_filename, local_filename, dest_domain_name)

            elif request_type.lower() == "multiwrite": # host start to leave the gossiping group
                local_filename, sdfs_filename, m = user_input.split(' ')[1], user_input.split(' ')[2], user_input.split(' ')[3]
                mems = list(fail_detector.membership_list.keys())
                random_vms = random.sample(mems, k = int(m))
                print(f"Target VMS: {str(random_vms)}")
                for dest_domain_name in random_vms:
                    send2Leader('put', sdfs_filename, local_filename, dest_domain_name)

            elif user_input.lower() == 'filetable':
                print(f"Files table is {str(filelocation_list)}")

            ################### MP2 COMMANDS ########################
            elif user_input.lower() == "leave": # host start to leave the gossiping group
                # Terminates gossiping thread here
                leave_status = True 
                logger.info("leave start")
                for gossiping_func, gossiping_thread in dict(fail_detector.gossiping_threadpool).items():
                    logger.info(f"joining {str(gossiping_func)}")
                    gossiping_thread.join()
                    logger.info(f"finished joining {str(gossiping_func)}")
                    del fail_detector.gossiping_threadpool[gossiping_func]
                assert len(fail_detector.gossiping_threadpool) == 0
                membership_list = {}

                #clean sockets
                for sock in fail_detector.gossiping_sockets.values():
                    sock.close()
                fail_detector.gossiping_sockets = {}
                logger.info(f"Finished leaving: {host_domain_name}")

            elif user_input.lower()[:4] == "join": # host join gossiping group
                # start gossiping thread here
                msg_drop_rate = float(user_input.lower().split(' ')[-1])/100 
                logger.writelog("Joined! Msg_drop_rate {}".format(str(msg_drop_rate)))
                leave_status = False
                suspicion_mode = False
                T_fail = 4
                fail_detector.gossiping()
                fail_detector.gossiping_threadpool['plot'] = threading.Thread(target=fail_detector.plot_report)
                fail_detector.gossiping_threadpool['plot'].start()

            elif user_input.lower() == 'suspicion': # host enable suspicion mode
                fail_detector.suspicion_mode = not fail_detector.suspicion_mode
                if fail_detector.suspicion_mode:
                    fail_detector.T_fail = 2
                else:
                    fail_detector.T_fail = 4
                logger.writelog("Suspicion mode is now {}".format(str(fail_detector.suspicion_mode)))

            elif user_input.lower()[0] == 'r': # host enable message drop
                fail_detector.msg_drop_rate = float(user_input.lower().split(' ')[-1]) / 100
                logger.writelog("Message rate is now {}".format(str(fail_detector.msg_drop_rate)))

            elif user_input.lower() == 'lm': # list current membership list
                fail_detector.memberlist_lock.acquire()
                logger.info("Membership List Now: {}".format(str(fail_detector.membership_list)))
                fail_detector.memberlist_lock.release()

            elif user_input.lower() == 'm': # sorted version of 'lm'
                # logger.info("lock debug: {}".format(str(debug_lock)))
                fail_detector.memberlist_lock.acquire()
                logger.info("Membership List Now: {}".format(str(sorted(membership_list.keys()))))
                fail_detector.memberlist_lock.release()
            elif request_type.lower() == "maple":
                maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory, interconnVal = user_input.split(' ')[1], user_input.split(' ')[2], user_input.split(' ')[3], user_input.split(' ')[4], user_input.split(' ')[5]
                current_maple_job['maple_exe'] = maple_exe
                current_maple_job['num_maples'] = num_maples
                current_maple_job['sdfs_intermediate_filename_prefix'] = sdfs_intermediate_filename_prefix
                current_maple_job['sdfs_src_directory'] = sdfs_src_directory

                http_packet = {}
                http_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                http_packet['request_source'] = host_domain_name
                http_packet['request_type'] ='maple'
                http_packet['maple_exe'] = maple_exe
                http_packet['num_maples'] = num_maples
                http_packet['interm'] = sdfs_intermediate_filename_prefix
                http_packet['source'] = sdfs_src_directory
                http_packet['intercon'] = interconnVal
                send(http_packet, 'maple', True)
                # maple_entrypoint(maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory)
            elif request_type.lower() == "juice":
            # juice <juice_exe> <num_juices>
            # <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>
            # delete_input={0,1} partition
                juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_src_directory, delete, partition = user_input.split(' ')[1], user_input.split(' ')[2], user_input.split(' ')[3], user_input.split(' ')[4], user_input.split(' ')[5], user_input.split(' ')[6]
                http_packet = {}
                http_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                http_packet['request_source'] = host_domain_name
                http_packet['request_type'] ='juice'
                http_packet['juice_exe'] = juice_exe
                http_packet['num_juices'] = num_juices
                http_packet['sdfs_interm'] = sdfs_intermediate_filename_prefix
                http_packet['sdfs_dir'] = sdfs_src_directory
                http_packet['delete'] = delete
                http_packet['partition'] = partition
                send(http_packet, 'juice', True)
            elif request_type.lower() == "select":
                # SELECT ALL FROM Dataset WHERE <regex condition>
                logger.info("received SELECT SQL command")
                type, f, dataset, where, regex = user_input.split(' ')[1], user_input.split(' ')[2], user_input.split(' ')[3], user_input.split(' ')[4], user_input.split(' ')[5]
                http_packet = {}
                http_packet['task_id'] = host_domain_name + '_'+str(datetime.datetime.now())  
                http_packet['request_source'] = host_domain_name
                http_packet['request_type'] ='SELECT'
                http_packet['dataset'] = dataset
                http_packet['regex'] = regex
                send(http_packet, 'SELECT', True)
            elif request_type.lower() == "JOIN":
                logger.info("received JOIN SQL command")
            else:
                print("Invalid Argument!")
        except Exception as e:
            print(str(e))

