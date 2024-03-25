from concurrent import futures
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
        self.commit_length = 0
        self.state = "Follower"

        self.leader_id = -1
        self.leader_lease = 5
        self.SERVERS_INFO = node_addresses

        self.heartbeat_duration = random.randint(100, 200) / 100
        self.timer = Timer(self.heartbeat_duration, self.leader)
        self.id = id
        self.threads = []
        self.timeout = 0
        self.no_of_heartbeats = 0
        
        if self.id == 0:
            self.leader_id = self.id
            self.state = "Leader"
        # else:
        #     self.leader_id = 1

        self.start()
    

    def start(self):
        # Checks if it is the leader or a follower
        # print("Starting server", self.id)
        if self.state == "Leader":
            # Using multitimers
            self.leader()
        else:
            self.follower()


    def leader(self):
        """
        Does the necessary actions after being declared as a leader.
        """
        try:
            start_time = time.time()
            if self.state != "Leader":
                print(f"Can not send heartbeats from a {self.state} node!")
                return

            self.heartbeat_duration = random.randint(100, 200) / 100
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
                self.no_of_heartbeats = 0
                print(f"Majority of the nodes are down! Leader {self.id} will stop!")
                self.leader_election()  # Perform leader election
                # self.current_term += 1
            else:
                self.no_of_heartbeats = 0
                # Restart the timer. For now, it is set to heartbeat_duration
                end_time = time.time()
                if end_time - start_time >= self.leader_lease:
                    # Leader Lease expired. Perform leader election
                    print("Leader Lease expired!")
                    self.leader_election()
                else:
                    # Renew Leader Lease by restarting the timer.
                    print("Renewing Leader Lease!")
            
            self.timer = Timer(self.heartbeat_duration, self.leader)
            # self.timer.join()
            self.timer.start()
            # self.timer.cancel()

        except Exception as e:
            print(f"Leader {self.id} stopped!")
    

    def leader_election(self):
        self.current_term += 1

    
    def follower(self):
        """
        Does the necessary actions after being declared as a follower.
        """
        pass


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
            print(f"Error in sending heartbeat to node {id}!")
            return None


    def AppendEntries(self, request, context):
        # print("Received heartbeat!", request.term, request.leaderId, request.logEntries, request.leaseDuration)

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
            self.log.append(logEntries[j])

        return response
 

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
    

    def handle_get_request(self, key):
        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            log = "GET " + key + " " + str(self.current_term)
            self.log.append(log)
            return raft_pb2.ServeClientResponse(data=self.dict.get(key, "Key not present!"), leader_id=self.leader_id, success=True)


    def handle_set_request(self, key, value):
        if self.state != "Leader":
            return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
        else:
            self.dict[key] = value
            log = "SET " + key + " " + value + " " + str(self.current_term)
            self.log.append(log)
            return raft_pb2.ServeClientResponse(data="Value set succesfully!", leader_id=self.leader_id, success=True)


    def ServeClient(self, request, context):
        # handle get, set, and getleader
        print(f"Received request: {request.request}")
        if request.request == "GETLEADER":
            return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "GET":
            key = request.request.split()[1]
            return self.handle_get_request(key)
        
        elif request.request.split()[0] == "SET":
            key = request.request.split()[1]
            value = request.request.split()[2]
            return self.handle_set_request(key, value)


    def is_up_to_date(self, last_log_index, last_log_term):
        if len(self.log) == 0:
            return True
        if self.log[-1].term < last_log_term:
            return True
        if self.log[-1].term == last_log_term and len(self.log) <= last_log_index:
            return True
        return False


def run_server():
    ID = int(sys.argv[1])

    try:
        node_addresses = {}
        with open('Config.conf') as f:
                lines = f.readlines()
                for line in lines:
                    parts = line.split()
                    id, address, port = parts[0], parts[1], parts[2]
                    node_addresses[int(id)] = f'{str(address)}:{str(port)}'
        
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(RaftNode(ID, node_addresses), server)

        server.add_insecure_port(node_addresses[ID])
        server.start()
        server.wait_for_termination()
    
    except KeyboardInterrupt:
        server.stop(0)
        print(f"Server {ID} stopped!")


if __name__ == '__main__':
    run_server()
