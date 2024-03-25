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
        
        self.id = id
        self.threads = []
        self.timeout = 0
        
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
            self.leader()
        else:
            self.follower()
    

    def leader(self):
        """
        Does the necessary actions after being declared as a leader.
        """
        print(f"Sending heartbeats from node {self.id}!")
        self.threads = []
        for id, address in self.SERVERS_INFO.items():
            if id == self.id: # Skip the leader itself
                continue
            self.threads.append(Thread(target=self.heartbeat, args=(id, address)))
        
        for t in self.threads:
            t.start()
            # t.join()

        # Restart the timer
        # self.timeout = random.randint(150, 300) / 1000
        # self.timer = Timer(self.timeout, self.leader)
        # self.timer.start()
        

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
            # logEntries can be a dictionary with id as s.no and value as the log entry
            request = raft_pb2.AppendEntriesRequest(term=self.current_term, leaderId=self.id, 
                                                    logEntries=entries,
                                                    leaseDuration=self.leader_lease)
            response = stub.AppendEntries(request)
            
            return response
        except Exception as e:
            print(f"Error in sending heartbeat to node {id}!")
            return None

    # def heartbeat(self, id, address):
    #     """
    #     Sends a heartbeat to a server.

    #     :param id: The id of the server to send the heartbeat to.
    #     :param address: The address of the server to send the heartbeat to.
    #     """
    #     if self.state != "Leader":
    #         return

    #     channel = grpc.insecure_channel(address)
    #     stub = raft_pb2_grpc.ServiceStub(channel)

    #     entries = []
    #     if self.nextIndex[id] <= len(self.log):
    #         entries = [self.log[self.nextIndex[id]-1]]

    #     prev_log_term = 0
    #     if self.nextIndex[id] > 1:
    #         prev_log_term = self.log[self.nextIndex[id]-2]["term"]

    #     message = raft_pb2.AppendTermIdMessage(term=int(self.term), id=int(self.id), prev_log_index=self.nextIndex[id]-1, 
    #                                            prev_log_term=prev_log_term, entries=entries, leader_commit=self.commitIndex)

    #     try:
    #         response = stub.AppendEntries(message)
    #         reciever_term = response.term
    #         reciever_result = response.result
    #         if reciever_term > self.term:
    #             self.update_term(reciever_term)
    #             self.set_timeout()
    #             self.follower_declaration()
    #         else:
    #             if reciever_result:
    #                 if len(entries) != 0:
    #                     self.matchIndex[id] = self.nextIndex[id]
    #                     self.nextIndex[id] += 1
    #             else:
    #                 self.nextIndex[id] -= 1
    #                 self.matchIndex[id] = min(
    #                     self.matchIndex[id], (self.nextIndex[id]-1))
    #     except grpc.RpcError:
    #         return


    # def leader_action(self):
        # """
        # Sends heartbeats to followers.
        # """
        # if self.state != "Leader":
        #     return

        # self.threads = []
        # for k, v in self.SERVERS_INFO.items():
        #     if k == self.id:
        #         continue
        #     self.threads.append(Thread(target=self.heartbeat, args=(k, v)))
        # for t in self.threads:
        #     t.start()

        # # self.restart_timer(self.timeout, self.leader_check)
    

    def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesResponse()
        print("Received heartbeat!", request)
        response.term = self.current_term
        response.success = True

        if request.term < self.current_term:
            response.success = False
        return response

        # self.leader_id = request.leader_id
        # self.current_term = request.term

        # if request.prev_log_index >= len(self.log) or self.log[request.prev_log_index].term != request.prev_log_term:
        #     response.success = False
        #     return response

        # self.log = self.log[:request.prev_log_index + 1] + request.entries
        # self.commit_length = min(request.leader_commit, len(self.log) - 1)
        # response.success = True
        # return response


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


    def ServeClient(self, request, context):
        # handle get, set, and getleader
        print(f"Received request: {request.request}")
        if request.request == "GETLEADER":
            return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "GET":
            key = request.request.split()[1]
            if self.state != "Leader":
                return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
            else:
                return raft_pb2.ServeClientResponse(data=self.dict.get(key, "Key not present!"), leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "SET":
            key = request.request.split()[1]
            value = request.request.split()[2]
            if self.state != "Leader":
                return raft_pb2.ServeClientResponse(data="Not the leader!", leader_id=self.leader_id, success=False)
            else:
                self.dict[key] = value
                log = request.request + " " + str(self.current_term)
                self.log.append(log)
                return raft_pb2.ServeClientResponse(data="Value set succesfully!", leader_id=self.leader_id, success=True)


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
