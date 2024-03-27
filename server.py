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
        self.log = []
        self.commitLogIndex = -1
        self.commitLogTerm = None
        self.dump = []
        self.commit_length = 0
        self.state = "Follower"
        self.no_of_nodes_receiving_append_entries_RPC = 1

        self.voted_for = None
        self.total_votes = 1

        self.leader_id = -1
        self.leader_lease = 6
        self.SERVERS_INFO = node_addresses
        self.heartbeat_duration = 2
        self.no_of_heartbeats = 1
        self.heartbeat_received = False

        self.timer = Timer(self.heartbeat_duration, self.leader)
        self.id = id
        self.threads = []
        self.timeout = random.randint(300, 750) / 100
        # print("Timeout:", self.timeout)
        
        self.handle_persistence()
        self.start()
        # print(f"Node {self.id} started!")
    

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
                self.commitLogTerm = self.log[-1]
                self.commitLogIndex = len(self.log) - 1
            
            # print("NODE_ID:", self.id)
            # print("DICT:", self.dict)
            # print("CURRENT_TERM:", self.current_term)
            # print("LOG:", self.log)


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
        # Checks if it is the leader or a follower
        if self.state == "Leader":
            # Using multitimers
            self.leader()
        else:
            self.follower()

# TODO: CORRECT THE LEADER LEASE DURATION END FROM HEARTBEAT. ALSO, CHECK THE LEADER LEASE DURATION IN THE REQUEST VOTE RPC.
    def write_content(self, content, dump=False, metadata=False, logs=False, verbose=True):
        if dump:
            path = f"logs_node_{self.id}/dump.txt"
        elif metadata:
            path = f"logs_node_{self.id}/metadata.txt"
        elif logs:
            path = f"logs_node_{self.id}/logs.txt"
        else:
            print("Error in writing to file: Did not specify the file to write to!")
            return

        if verbose:
            print("-"*25)
            print(content)
        
        with open(path, "a") as f:
            f.write(content + "\n")


    def leader(self):
        """
        Sends heartbeats to all the servers in the network. If the leader lease expires, then it starts the leader election. If majority of the nodes are down, then it stops.
        """
        try:
            print("Leader dict:", self.dict)
            # self.write_content(f"Node {self.id} became the leader for term {self.current_term}", dump=True)
            self.write_content(f"Leader {self.leader_id} sending heartbeat & Renewing Lease", dump=True)
            start_time = time.time()
            if self.state != "Leader":
                print(f"Can not send heartbeats from a {self.state} node!")
                return

            s = time.time()
            # print(f"Sending heartbeats from node {self.id}!")

            threads_election = []
            for id, address in self.SERVERS_INFO.items():
                if id == self.id: # Skip the leader itself
                    continue
                threads_election.append((id, Thread(target=self.heartbeat, args=(id, address))))
            
            for id, t in threads_election:
                t.start()
            
            for id, t in threads_election:
                t.join()
                print(f"L:Node {id} joined!")
            
            # Check if majority of the nodes are alive
            e = time.time()
            print("No of heartbeats:", self.no_of_heartbeats, ", after time:", round(e - s, 3))

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
                    # print("Renewing Leader Lease!")

                    # It will commit the log entries that are not committed yet.
                    for i in range(self.commitLogIndex+1, len(self.log)):
                        if "SET" in self.log[i]:
                            key, value = self.log[i].split()[1], self.log[i].split()[2]
                            self.dict[key] = value
                        self.write_content(self.log[i], logs=True)
                        self.write_content(f"Node {self.id} (leader) committed the entry {self.log[i]} to the state machine", dump=True)
                        self.commitLogIndex = i
                        self.commitLogTerm = self.log[i]
                    
                    self.timer = Timer(self.heartbeat_duration, self.leader)
                    self.timer.start()

        except Exception as e:
            print(f"Leader {self.id} stopped!")
    

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
                                                    logEntries=entries, leaseDuration=self.leader_lease, 
                                                    prevLogIndex=self.commitLogIndex, prevLogTerm=self.commitLogTerm)
            # print("Request:", request.term, request.leaderId, request.logEntries, request.leaseDuration)
            response = stub.AppendEntries(request)
            if response.success:
                self.no_of_heartbeats += 1
            
            return response
        except Exception as e:
            # print(f"Error in sending heartbeat to node {id}!")
            self.write_content(f"Error occurred while sending RPC to Node {id}.", dump=True)
            return None


    def reset_term_variables(self):
        """
        Resets the term variables.
        """
        self.current_term += 1
        self.voted_for = None
        self.total_votes = 1
        self.leader_id = -1
        self.heartbeat_received = False
        self.leader_id = -1
        self.state = "Follower"
        self.no_of_heartbeats = 1


    def restart_timer(self):
        """
        Restarts the timer.
        """
        self.timer.cancel()
        self.timeout = random.randint(300, 750) / 100
        self.timer = Timer(self.timeout, self.leader_election)
        self.timer.start()


    def leader_election(self):
        """
        Performs leader election.
        """
        # print(f"Leader election started by node {self.id}!")
        if self.heartbeat_received:
            self.state = "Follower"
            print(f"Received heartbeat from the leader! Node {self.id} is not the leader!")
            self.restart_timer()
            return
        
        self.write_content(f"Node {self.id} election timer timed out, Starting election.", dump=True)
        self.reset_term_variables()

        # print(f"Node {self.id} Timeout! Leader election will be started for Term {self.current_term}!")
        self.state = "Candidate"
        self.voted_for = self.id
        self.timer.cancel()     # Cancels the timeout timer

        threads = []
        for id, address in self.SERVERS_INFO.items():
            if id == self.id:
                continue
            threads.append(Thread(target=self.request_vote, args=(id, address)))

        for t in threads:
            t.start()

        for t in threads:
            t.join()
        
        votes_received = self.total_votes
        self.total_votes = 1

        if votes_received > len(self.SERVERS_INFO) // 2:
            # Wait for leader lease duration to receive heartbeats from the leader
            start_time = time.time()
            while time.time() - start_time < self.leader_lease:
                if self.heartbeat_received:
                    # self.heartbeat_received = False
                    break
            
            if self.heartbeat_received:
                self.heartbeat_received = False
                self.state = "Follower"
                print(f"Received heartbeat from the leader! Node {self.id} is not the leader!")
        
            else:
                self.state = "Leader"
                self.leader_id = self.id
                print(f"Node {self.id} is the leader!")
                
        else:
            self.state = "Follower"
            print(f"No leader elected in term {self.current_term}!")
        
        self.start()
    
    
    def request_vote(self, id, address):
        """
        Sends a request to all the servers in the network to vote for the current server.
        """
        try:
            channel = grpc.insecure_channel(address)
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            if len(self.log) == 0:
                request = raft_pb2.RequestVoteRequest(term=self.current_term, candidateId=self.id, lastLogIndex=0, lastLogTerm=0)
            else:
                request = raft_pb2.RequestVoteRequest(term=self.current_term, candidateId=self.id, lastLogIndex=len(self.log)-1, lastLogTerm=self.log[-1])
            response = stub.RequestVote(request)
            # print("Response:", response.term, response.voteGranted, response.leaseDuration)
            if response.voteGranted:
                # print(f"Vote granted by node {id}!")
                self.write_content(f"Vote granted for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)
                self.total_votes += 1
            else:
                print(f"Vote NOT granted by node {id}!")
                # self.write_content(f"Vote denied for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)
        
        except Exception as e:
            # print(f"Error in requesting vote from node {id}!")
            self.write_content(f"Vote denied for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)


    def follower(self):
        """
        Waits for the heartbeat from the leader. If the heartbeat is not received, then it starts the leader election.
        """
        try:
            # while self.state == "Follower":
            self.timer.cancel()
            self.timer=Timer(self.timeout, self.leader_election)
            self.timer.start()
        
        except Exception as e:
            print(f"Follower {self.id} stopped!")

    
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
            self.restart_timer()
            print("Log entries:", request.logEntries)   # Received the log entries from the leader

            # Update the log
            logEntries = request.logEntries.split('\n')
            index = request.commitLogIndex
            term = request.commitLogTerm

            # Update the follower's log. This code assumes that everything before the commitLogIndex is in sync with the majority of the nodes.
            if self.commitLogIndex < index:
                # Follower has some entries that leader has committed but the follower has not committed yet.
                for i in range(self.commitLogIndex+1, index+1):
                    # Follower does not have the entry that leader has committed. May happen to crashed/out-of-sync followers. Commit entries that leader has committed.
                    if len(self.log) < index:
                        self.log.append(logEntries[i])
                    if "SET" in logEntries[i]:
                        key, value = logEntries[i].split()[1], logEntries[i].split()[2]
                        self.dict[key] = value
                    self.write_content(logEntries[i], logs=True)
                    self.write_content(f"Node {self.id} (follower) committed the entry {logEntries[i]} to the state machine", dump=True)
                    self.commitLogIndex = i
                    self.commitLogTerm = logEntries[i]
            
            else:
                # Follower has already committed the entries that leader has committed. Put the leader's uncommitted entries in the follower's log.
                for i in range(index+1, len(logEntries)):
                    self.log.append(logEntries[i])
            
            return response
        
        except Exception as e:
            self.write_content(f"Node {self.id} rejected AppendEntries RPC from {self.leader_id}.", dump=True)  
            # TODO: Missing a case where the first append entry is sent to recognise the leader. Hence, dumps would have the previous leader_id.
            return None
 

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()
        response.term = self.current_term
        response.leaseDuration = self.leader_lease
        response.voteGranted = False


        if self.state == "Leader":
            return response
        
        
        # Restarting the timer for the follower
        print("Timeout:", self.timeout)
        self.timer.cancel()
        self.timer = Timer(self.timeout+2, self.leader_election)
        # self.restart_timer()
        # self.timer.start()

        # print(f"Received vote request from node {request.candidateId} with term {request.term}!")

        if request.term < self.current_term:
            print("Candidate term is less than current term!")
            response.voteGranted = False
            # print(f"Vote NOT granted to node {request.candidateId}!")
            print("Voted for:", self.voted_for, "ID:", self.id, "Candidate ID:", request.candidateId)
            return response
        # else:
        #     self.current_term = request.term
        #     self.voted_for = None
        #     response.voteGranted = True
        #     return response
        # print("Trying to vote!")
        if self.voted_for is None or self.voted_for == self.id:
            if self.is_up_to_date(request.lastLogIndex, request.lastLogTerm):
                self.voted_for = request.candidateId
                response.voteGranted = True
            else:
                response.voteGranted = False
        else:
            print(f"Already voted for node {self.voted_for}!")
            response.voteGranted = False

        print("Voted for:", self.voted_for, "ID:", self.id, "Candidate ID:", request.candidateId)
        self.voted_for = None
        return response
    
    
    def is_up_to_date(self, last_log_index, last_log_term):
        return True
        if len(self.log) == 0:
            return True
        if self.log[-1].term < last_log_term:
            return True
        if self.log[-1].term == last_log_term and len(self.log) <= last_log_index:
            return True
        return False


    def handle_get_request(self, request):
        key = request.split()[1]

        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            self.write_content(f"Node {self.leader_id} (leader) received a {request} request.", dump=True)

            # Append it into leader's log (Not committed yet).
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

            # Append it into leader's log (Not committed yet).
            log = "SET " + key + " " + value + " " + str(self.current_term)
            self.log.append(log)

            return raft_pb2.ServeClientResponse(data="Value set succesfully!", leader_id=self.leader_id, success=True)

# TODO: NOT PERSISTING THE ENTIRE LOG. ONLY COMMITTED ENTRIES ARE PERSISTED.
    def ServeClient(self, request, context):
        # handle get, set, and getleader
        print(f"Received request: {request.request}")
        if request.request == "GETLEADER":
            if self.id == self.leader_id:   # Leader appends the log
                self.log.append("NO OP " + str(self.current_term))
            return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "GET":
            return self.handle_get_request(request.request)
        
        elif request.request.split()[0] == "SET":
            return self.handle_set_request(request.request)


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
