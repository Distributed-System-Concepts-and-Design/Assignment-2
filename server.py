from concurrent import futures
import os
import random
import grpc
import raft_pb2
import raft_pb2_grpc
import time, sys
from threading import Thread, Timer

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, id, node_addresses):
        self.dict = {}
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.dump = []
        self.commit_length = 0
        self.state = "Follower"

        self.leader_id = -1
        self.leader_lease = 5
        self.SERVERS_INFO = node_addresses
        self.heartbeat_duration = 1
        self.no_of_heartbeats = 1

        self.timer = Timer(self.heartbeat_duration, self.leader)
        self.id = id
        self.threads = []
        self.timeout = 0
        
        if self.id == 0:
            self.leader_id = self.id
            self.state = "Leader"

        self.start()
    

    def handle_persistence(self):
        # Create directory logs_node_id if it does not exist
        if not os.path.exists(f"logs_node_{self.id}"):
            os.mkdir(f"logs_node_{self.id}")
        

        # Create files logs.txt, metadata.txt and dump.txt if they do not exist
        path = f"logs_node_{self.id}" + "/logs.txt"
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write("")
        else:
            # Read the logs from the file
            with open(path, "r") as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip() == "": 
                        continue
                    line_ = line.strip()
                    
                    # Initialise self.dict with the SET commands in the logs
                    if line_.split()[0] == "SET":
                        key, value = line_.split()[1], line_.split()[2]
                        self.dict[key] = value
                    
                    self.log.append(line_)
            
            # Initialise current_term with the last term in the logs
            if len(self.log) > 0:
                self.current_term = int(self.log[-1].split()[-1])
            
            print("NODE_ID:", self.id)
            print("DICT:", self.dict)
            print("CURRENT_TERM:", self.current_term)
            print("LOG:", self.log)


        path = path.split('/')[0] + "/metadata.txt"
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write("")
        else:
            # Read the metadata from the file
            with open(path, "r") as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip() == "": 
                        continue
                    # self.current_term = int(line.strip())
        
        
        path = path.split('/')[0] + "/dump.txt"
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write("")
        else:
            # Read the dump from the file
            with open(path, "r") as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip() == "": 
                        continue
                    self.dump.append(line.strip())


    def start(self):
        self.handle_persistence()
        
        # Checks if it is the leader or a follower
        if self.state == "Leader":
            # Using multitimers
            self.leader()
        else:
            self.follower()


    def write_content(self, content, dump=False, metadata=False, logs=False):
        if dump:
            path = f"logs_node_{self.id}/dump.txt"
        elif metadata:
            path = f"logs_node_{self.id}/metadata.txt"
        elif logs:
            path = f"logs_node_{self.id}/logs.txt"
        else:
            print("Error in writing to file: Did not specify the file to write to!")
            return

        print("-"*25)
        print(content)
        
        with open(path, "a") as f:
            f.write(content + "\n")


    def leader(self):
        """
        Does the necessary actions after being declared as a leader.
        """
        try:
            # self.write_content(f"Node {self.id} became the leader for term {self.current_term}", dump=True)
            self.write_content(f"Leader {self.leader_id} sending heartbeat & Renewing Lease", dump=True)
            start_time = time.time()
            if self.state != "Leader":
                print(f"Can not send heartbeats from a {self.state} node!")
                return

            print(f"Sending heartbeats from node {self.id}!")

            self.threads = []
            for id, address in self.SERVERS_INFO.items():
                if id == self.id: # Skip the leader itself
                    continue
                self.threads.append(Thread(target=self.heartbeat, args=(id, address)))
                # self.threads.append(Timer(self.heartbeat_duration, self.heartbeat, args=(id, address)))
            
            for t in self.threads:
                t.start()
            
            for t in self.threads:
                t.join()
            
            # Check if majority of the nodes are alive
            print("No of heartbeats:", self.no_of_heartbeats)

            if self.no_of_heartbeats <= len(self.SERVERS_INFO) // 2:
                self.no_of_heartbeats = 1
                print(f"Majority of the nodes are down! Leader {self.id} will stop!")
                self.write_content(f"Leader {self.leader_id} lease renewal failed. Stepping Down.", dump=True)
                self.leader_election()  # Perform leader election
            else:
                self.no_of_heartbeats = 1
                # Restart the timer. For now, it is set to heartbeat_duration
                end_time = time.time()
                if end_time - start_time + self.heartbeat_duration >= self.leader_lease:
                    # Leader Lease expired. Perform leader election
                    print("Leader Lease expired!")
                    self.write_content(f"Leader {self.leader_id} lease renewal failed. Stepping Down.", dump=True)
                    self.leader_election()
                else:
                    # Renew Leader Lease by restarting the timer.
                    print("Renewing Leader Lease!")
                    # Committing the logs
                    for log in self.log:
                        self.write_content(log, logs=True)
            
            self.timer = Timer(self.heartbeat_duration, self.leader)
            self.timer.start()

        except Exception as e:
            print(f"Leader {self.id} stopped!")


    def leader_election(self):
        self.current_term += 1
        self.write_content(f"Node {self.id} became the leader for term {self.current_term}.", dump=True)

    
    def follower(self):
        """
        Does the necessary actions after being declared as a follower.
        """
        self.state = "Follower"


    def heartbeat(self, id, address):
        """
        Sends a heartbeat to a server.

        :param id: The id of the server to send the heartbeat to.
        :param address: The address of the server to send the heartbeat to.
        """
        try:
            if self.state != "Leader":
                print(f"Can not send heartbeat from a {self.state} node!")
                return
            
            channel = grpc.insecure_channel(address)
            stub = raft_pb2_grpc.RaftNodeStub(channel)
 
            # Formatting the entries
            entries = '\n'.join(self.log)
            # logEntries can be a dictionary with id as s.no and value as the log entry. But currently it is treated as list of strings (logs).
            request = raft_pb2.AppendEntriesRequest(term=self.current_term, leaderId=self.id, 
                                                    logEntries=entries,
                                                    leaseDuration=self.leader_lease)
            # print("Request:", request.term, request.leaderId, request.logEntries, request.leaseDuration)
            response = stub.AppendEntries(request)
            if response.success:
                self.no_of_heartbeats += 1
            
            return response
        except Exception as e:
            # print(f"Error in sending heartbeat to node {id}!")
            self.write_content(f"Error occurred while sending RPC to Node {id}.", dump=True)
            return None


    def AppendEntries(self, request, context):
        try:
            self.write_content(f"Node {self.id} accepted AppendEntries RPC from {request.leaderId}.", dump=True)
            response = raft_pb2.AppendEntriesResponse()
            response.term = self.current_term   # Stores previous term
            response.success = True             # Bullshit value

            self.current_term = request.term
            self.leader_id = request.leaderId
            self.leaseDuration = request.leaseDuration

            print(f"Received heartbeat from node {request.leaderId} with term {request.term}!")
            print("Log entries:", request.logEntries)

            # Update the log
            logEntries = request.logEntries.split('\n')

            # Iterate over the list from the back and find out the first entry that is also present in the log.
            for i in range(len(logEntries)-1, -1, -1):
                if logEntries[i] in self.log:
                    break
            
            # Append the entries that are not present in the log.
            for j in range(i+1, len(logEntries)):
                if self.id == self.leader_id:
                    self.write_content(f"Node {self.id} (leader) committed the entry {logEntries[j]} to the state machine", dump=True)
                else:
                    self.write_content(f"Node {self.id} (follower) committed the entry {logEntries[j]} to the state machine", dump=True)
                
                self.write_content(logEntries[j], logs=True)
                self.log.append(logEntries[j])
            
            return response
        
        except Exception as e:
            self.write_content(f"Node {self.id} rejected AppendEntries RPC from {self.leader_id}.", dump=True)  
            # TODO: Missing a case where the first append entry is sent to recognise the leader. Hence, dumps would have the previous leader_id.
            return None
 

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()
        response.term = self.current_term

        if request.term < self.current_term:
            response.vote_granted = False
            return response

        if self.voted_for is None or self.voted_for == request.candidate_id:
            if self.is_up_to_date(request.last_log_index, request.last_log_term):
                self.voted_for = request.candidate_id
                response.vote_granted = True
            else:
                response.vote_granted = False
        else:
            response.vote_granted = False

        return response
    

    def handle_get_request(self, request):
        key = request.split()[1]

        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            log = "GET " + key + " " + str(self.current_term)
            self.log.append(log)
            self.write_content(f"Node {self.leader_id} (leader) received a {request} request.", dump=True)
            return raft_pb2.ServeClientResponse(data=self.dict.get(key, "Key not present!"), leader_id=self.leader_id, success=True)


    def handle_set_request(self, request):
        key = request.split()[1]
        value = request.split()[2]

        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            self.write_content(f"Node {self.leader_id} (leader) received a {request} request.", dump=True)
            self.dict[key] = value
            log = "SET " + key + " " + value + " " + str(self.current_term)
            self.log.append(log)
            return raft_pb2.ServeClientResponse(data="Value set succesfully!", leader_id=self.leader_id, success=True)


    def ServeClient(self, request, context):
        # handle get, set, and getleader
        print(f"Received request: {request.request}")
        if request.request == "GETLEADER":
            self.log.append("NO OP "+str(self.current_term))
            return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "GET":
            return self.handle_get_request(request.request)
        
        elif request.request.split()[0] == "SET":
            return self.handle_set_request(request.request)


    def is_up_to_date(self, last_log_index, last_log_term):
        if len(self.log) == 0:
            return True
        if self.log[-1].term < last_log_term:
            return True
        if self.log[-1].term == last_log_term and len(self.log) <= last_log_index:
            return True
        return False


def run_server():
    try:
        ID = int(sys.argv[1])
    except IndexError as e:
        print("ERROR: Node ID not provided as an argument!")
        return

    try:
        node_addresses = {}
        with open('Config.conf') as f:
                lines = f.readlines()
                for line in lines:
                    parts = line.split()
                    id, address, port = parts[0], parts[1], parts[2]
                    node_addresses[int(id)] = f'{str(address)}:{str(port)}'
        
        if ID not in node_addresses:
            print(f"ERROR: Node {ID} not present in Config.conf!")
            return
        
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        obj = RaftNode(ID, node_addresses)
        raft_pb2_grpc.add_RaftNodeServicer_to_server(obj, server)

        server.add_insecure_port(node_addresses[ID])
        server.start()
        server.wait_for_termination()
    
    except KeyboardInterrupt:
        server.stop(0)
        print(f"Server {ID} stopped!")


if __name__ == '__main__':
    run_server()
