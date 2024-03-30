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
        self.commitLogIndex = -1
        self.commitLogTerm = None
        self.log = []
        self.dump = []
        self.state = "Follower"

        self.voted_for = None
        self.total_votes = 1

        self.leader_id = -1
        self.leader_lease = 5
        self.SERVERS_INFO = node_addresses
        self.heartbeat_duration = 2
        self.no_of_heartbeats = 1
        self.heartbeat_received = False

        self.timer = Timer(self.heartbeat_duration, self.leader)
        self.leaseTimer = Timer(self.leader_lease, self.leader_step_down)
        self.id = id
        self.threads = []
        self.timeout_duration = (600, 1000)
        self.timeout = random.randint(self.timeout_duration[0], self.timeout_duration[1]) / 100

        self.start_time = time.time()
        self.end_time = time.time()
        
        print(f"Node {self.id} started!")
        self.handle_persistence()
        # print("Current Term:", self.current_term)
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
        

        path = path.split('/')[0] + "/metadata.txt"
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write("")
        else:
            # Read the metadata from the file
            # Metadata: Term 0 CommitIndex 1 votedFor 2 \n Term 0 CommitIndex 1 votedFor 2 \n ...
            with open(path, "r") as f:
                lines = f.readlines()

                for line in lines:
                    line = line.strip()
                    if line == "": 
                        continue

                    parts = line.split(', ')
                    for part_ in parts:
                        if part_ == "": 
                            continue
                        part = part_.split()
                        if part[0] == "Term":
                            self.current_term = int(part[1])
                        elif part[0] == "CommitIndex":
                            self.commitLogIndex = int(part[1])
                        elif part[0] == "VotedFor":
                            if part[1] == "None":
                                self.voted_for = None
                            else:
                                self.voted_for = int(part[1])
            
            if self.commitLogIndex == len(self.log) - 1 and self.commitLogIndex != -1:
                print("Metadata and Log are in conistency!")
                self.commitLogTerm = self.log[self.commitLogIndex].split()[-1]
            elif self.commitLogIndex != -1:
                print("ERROR: COMMITLOGINDEX OF METADATA AND LOG ARE INCONSISTENT!")
                print("Metadata CommitLogIndex:", self.commitLogIndex, "Log CommitLogIndex:", len(self.log)-1)
            else:
                print("No logs present in the file! Thus, self.commitLogIndex is -1!")
        

        # DUMP.TXT: Stores the dump of the node
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
        self.voted_for = None


    def print_state(self):
        print('-'*25)
        print(f"Node {self.id} Term {self.current_term}")
        print(f"Node {self.id} CommitLogIndex {self.commitLogIndex}")
        print(f"Node {self.id} VotedFor {self.voted_for}")
        print(f"Node {self.id} Leader {self.leader_id}")
        print(f"Node {self.id} State {self.dict}")
        print('-'*25)


    def start(self):
        # Checks if it is a leader or a follower
        self.print_state()
        if self.state == "Leader":

            self.leaseTimer.cancel()
            self.leaseTimer = Timer(self.leader_lease, self.leader_step_down)
            self.log.append(f"NO-OP {self.current_term}")
            self.leaseTimer.start()

            self.leader()
        else:
            self.follower()


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
        
        # Write the content to the file
        with open(path, "a") as f:
            f.write(content + "\n")


    def leader(self):
        """
        Sends heartbeats to all the servers in the network. If the leader lease expires, then it starts the leader election.
        """
        try:
            self.write_content(f"Leader {self.leader_id} sending heartbeat & Renewing Lease", dump=True)
            start_time = time.time()
            if self.state != "Leader":
                print(f"Can not send heartbeats from a {self.state} node!")
                return

            print("Leader Log:", self.log)

            threads_election = []
            for id, address in self.SERVERS_INFO.items():
                if id == self.id: # Skip the leader itself
                    continue
                threads_election.append((id, Thread(target=self.heartbeat, args=(id, address))))
            
            for id, t in threads_election:
                t.start()
            
            for id, t in threads_election:
                t.join()
            
            # Check if majority of the nodes are alive
            print("No of heartbeats:", self.no_of_heartbeats)

            if self.no_of_heartbeats <= len(self.SERVERS_INFO) // 2:
                self.no_of_heartbeats = 1
                print(f"Majority of the nodes are down! Leader {self.id} will retry!")
            else:
                self.no_of_heartbeats = 1
                # Restart the timer. For now, it is set to heartbeat_duration
                # Renew Leader Lease by restarting the timer.
                self.leaseTimer.cancel()
                self.leaseTimer = Timer(self.leader_lease, self.leader_step_down)
                
                # It will commit the log entries that are not committed yet.
                for i in range(self.commitLogIndex+1, len(self.log)):
                    if "SET" in self.log[i]:
                        key, value = self.log[i].split()[1], self.log[i].split()[2]
                        self.dict[key] = value
                    self.write_content(self.log[i], logs=True)
                    self.write_content(f"Node {self.id} (leader) committed the entry {self.log[i]} to the state machine", dump=True)
                    self.commitLogIndex = i
                    self.commitLogTerm = self.log[i]
                self.write_content(f"Term {self.current_term}, CommitIndex {self.commitLogIndex}, VotedFor {self.voted_for}", metadata=True)

                self.leaseTimer.start()
            
            self.timer = Timer(self.heartbeat_duration, self.leader)
            self.timer.start()

        except Exception as e:
            print(e)
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
            
            request = raft_pb2.AppendEntriesRequest(term=self.current_term, leaderId=self.id, leaseDuration=self.leader_lease,
                                                    commitLogIndex=self.commitLogIndex, commitLogTerm=self.commitLogTerm)
 

            for log in self.log:
                item = request.logEntries.add()
                item.entries = log
            
            response = stub.AppendEntries(request)
            if response.success:
                self.no_of_heartbeats += 1
            
            return response
        except Exception as e:
            self.write_content(f"Error occurred while sending RPC to Node {id}.", dump=True)
            return None


    def AppendEntries(self, request, context):
        try:
            self.write_content(f"Node {self.id} accepted AppendEntries RPC from {self.leader_id}.", dump=True)
            response = raft_pb2.AppendEntriesResponse()
            response.term = self.current_term   # Stores previous term
            response.success = True             # Bullshit value

            # if request.term < self.current_term:    # Leader has a lower term than follower. Reject the RPC.
            #     self.write_content(f"Node {self.id} rejected AppendEntries (DUE TO OUTDATED LEADER) RPC from {self.leader_id}.", dump=True)
            #     response.success = False
            #     return response
            # else:
            #     self.write_content(f"Node {self.id} accepted AppendEntries RPC from {self.leader_id}.", dump=True)
            #     response.success = True

            self.current_term = request.term
            self.leader_id = request.leaderId
            self.leaseDuration = request.leaseDuration

            # print(f"Received heartbeat from node {request.leaderId} with term {request.term}!")
            # self.heartbeat_received = True
            self.restart_timer()

            # Update the log
            logEntries = [entry.entries for entry in request.logEntries]
            index = request.commitLogIndex
            term = request.commitLogTerm


            # Update the follower's log. This code assumes that everything before the commitLogIndex is in sync with the majority of the nodes.
            if self.commitLogIndex < index:
                # Follower has some entries that leader has committed but the follower has not committed yet.
                for i in range(self.commitLogIndex+1, index+1):

                    # Follower does not have the entry that leader has committed. May happen to crashed/out-of-sync followers. Commit entries that leader has committed.
                    if len(self.log) <= index:
                        self.log.append(logEntries[i])
                    else:
                        self.log[i] = logEntries[i]
                    
                    if "SET" in logEntries[i]:
                        key, value = logEntries[i].split()[1], logEntries[i].split()[2]
                        self.dict[key] = value
                    
                    self.write_content(logEntries[i], logs=True)
                    self.write_content(f"Node {self.id} (follower) committed the entry {logEntries[i]} to the state machine", dump=True)
                    self.commitLogIndex = i
                    self.commitLogTerm = logEntries[i]
            
            else:
                # Put the leader's uncommitted entries in the follower's log.
                for i in range(index+1, len(logEntries)):
                    # Check if any value is already present in the log.
                    if len(self.log) > i:   # Rollback/Rewrite the (uncommitted) log entries for the follower.
                        self.log[i] = logEntries[i]
                    else:
                        self.log.append(logEntries[i])
            
            print("Log entries:", self.log)   # Received the log entries from the leader
            # Updating metadata entries
            self.write_content(f"Term {self.current_term}, CommitIndex {self.commitLogIndex}, VotedFor {self.voted_for}", metadata=True)

            return response
        
        except Exception as e:
            self.write_content(f"Node {self.id} rejected AppendEntries RPC from {self.leader_id}.", dump=True)
            # self.write_content(f"Error occurred while sending RPC to Node {id}.", dump=True)
            response = raft_pb2.AppendEntriesResponse()
            response.success = False
            return response


    def reset_term_variables(self):
        """
        Resets the term variables.
        """
        self.current_term += 1
        self.voted_for = None
        self.total_votes = 1
        self.leader_id = -1
        self.heartbeat_received = False
        self.no_of_heartbeats = 1


    def restart_timer(self):
        """
        Restarts the timer.
        """
        self.timer.cancel()
        self.timeout = random.randint(self.timeout_duration[0], self.timeout_duration[1]) / 100
        self.timer = Timer(self.timeout, self.leader_election)
        self.timer.start()


    def leader_step_down(self):
        """
        Steps down the leader.
        """
        self.leaseTimer.cancel()
        self.state = "Follower"
        self.write_content(f"Leader {self.leader_id} lease renewal failed. Stepping Down.", dump=True)
        # self.write_content(f"{self.id} Stepping down.", dump=True)
        # self.heartbeat_received = False
        self.restart_timer()
        # self.leader_election()


    def leader_election(self):
        """
        Performs leader election.
        """
        self.leaseTimer.cancel()
        self.end_time = time.time()
        # print("Time taken:", self.end_time - self.start_time)
        if self.heartbeat_received:
            self.state = "Follower"
            print(f"Received heartbeat from the leader! Node {self.id} is not the leader!")
            self.restart_timer()
            return
        
        self.write_content(f"Node {self.id} election timer timed out, Starting election.", dump=True)
        self.reset_term_variables()
        # print(f"TIMED OUT after {self.timeout} seconds at {self.end_time}! Actual time taken: {round(self.end_time - self.start_time, 3)} seconds!")
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
            if self.leader_id != -1:
                self.write_content(f"New Leader waiting for Old Leader Lease to timeout.", dump=True)
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
                self.write_content(f"Node {self.id} became the leader for term {self.current_term}.", dump=True)
        
        else:
            self.state = "Follower"
            print(f"No leader elected in term {self.current_term}!")
        
        
        self.write_content(f"Term {self.current_term}, CommitIndex {self.commitLogIndex}, VotedFor {self.voted_for}", metadata=True)
        self.start()
    

    def follower(self):
        """
        Waits for the heartbeat from the leader. If the heartbeat is not received, then it starts the leader election.
        """
        try:
            self.timer.cancel()
            self.timer=Timer(self.timeout, self.leader_election)
            self.timer.start()
            self.start_time = time.time()
        
        except Exception as e:
            print(f"Follower {self.id} stopped!")
 

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
                term_ = int(self.log[-1].split()[-1])
                request = raft_pb2.RequestVoteRequest(term=self.current_term, candidateId=self.id, lastLogIndex=len(self.log)-1, lastLogTerm=term_)
            response = stub.RequestVote(request)
            if response.voteGranted:
                # print(f"Vote granted by node {id}!")
                self.write_content(f"Vote granted for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)
                self.total_votes += 1
            else:
                # print(f"Vote NOT granted by node {id}!")
                self.write_content(f"Vote denied for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)
        
        except Exception as e:
            # print(f"Error in requesting vote from node {id}!")
            # self.write_content(f"Vote denied for Node {self.id} by Node {id} in term {self.current_term}.", dump=True)
            self.write_content(f"Error occurred while sending RPC to Node {id}.", dump=True)


    def RequestVote(self, request, context):
        print("Vote Request Received!")
        self.timer.cancel()
        self.timer.join()

        response = raft_pb2.RequestVoteResponse()
        response.term = self.current_term
        response.leaseDuration = self.leader_lease
        response.voteGranted = False

        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = "Follower"
        
        lastTerm = 0

        if len(self.log) > 0:
            lastTerm = int(self.log[-1].split()[-1])

        logOK = (request.lastLogTerm > lastTerm) or (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log)-1)
        
        # print(f"Log OK {self.current_term}:", logOK, "Last Log Term:", lastTerm, "Last Log Index:", len(self.log)-1, 
        #        "Request Last Log Term:", request.lastLogTerm, "Request Last Log Index:", request.lastLogIndex)
        # print(f"Log OK {self.current_term}:", (request.lastLogTerm > lastTerm), (request.lastLogTerm == lastTerm), request.lastLogIndex >= len(self.log)-1)
        
        if request.term == self.current_term and logOK and (self.voted_for is None or self.voted_for == request.candidateId):
            response.voteGranted = True
            self.voted_for = request.candidateId
        else:
            response.voteGranted = False
        

        self.timer = Timer(self.timeout, self.leader_election)
        self.timer.start()
        self.start_time = time.time()
        print("Voted for:", self.voted_for, "ID:", self.id, "Candidate ID:", request.candidateId, "Updated Term:", self.current_term)

        return response


    def handle_get_request(self, request):
        key = request.split()[1]

        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            self.write_content(f"Node {self.leader_id} (leader) received a {request} request.", dump=True)
            
            log = "GET " + key + " " + str(self.current_term)
            self.log.append(log)

            return raft_pb2.ServeClientResponse(data=self.dict.get(key, "Key not present!"), leader_id=self.leader_id, success=True)


    def handle_set_request(self, request):
        key = request.split()[1]
        value = request.split()[2]

        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            self.write_content(f"Node {self.leader_id} (leader) received a {request} request.", dump=True)
            
            log = "SET " + key + " " + value + " " + str(self.current_term)
            self.log.append(log)

            return raft_pb2.ServeClientResponse(data="Value set succesfully!", leader_id=self.leader_id, success=True)


    def ServeClient(self, request, context):
        # handle get, set, and getleader
        try:
            # print(f"Received request: {request.request}")
            req = request.request.split()[0]

            if req == "GETLEADER":
                return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
            
            elif req == "GET":
                return self.handle_get_request(request.request)
            
            elif req == "SET":
                return self.handle_set_request(request.request)
        except Exception as e:
            return raft_pb2.ServeClientResponse(data="Error occurred!", leader_id=self.leader_id, success=False)


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
