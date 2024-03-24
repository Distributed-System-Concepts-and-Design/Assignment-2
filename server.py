from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc
import time, sys

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, id, node_addresses):
        self.dict = {}
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_length = 0
        self.state = "Leader"
        self.leader_id = -1
        self.node_addresses = node_addresses
        self.id = id
        self.leader_id = self.id
        self.term_id = 0


    def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesResponse()
        response.term = self.current_term

        if request.term < self.current_term:
            response.success = False
            return response

        self.leader_id = request.leader_id
        self.current_term = request.term

        if request.prev_log_index >= len(self.log) or self.log[request.prev_log_index].term != request.prev_log_term:
            response.success = False
            return response

        self.log = self.log[:request.prev_log_index + 1] + request.entries
        self.commit_length = min(request.leader_commit, len(self.log) - 1)
        response.success = True
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


    def ServeClient(self, request, context):
        # handle get, set, and getleader
        print(f"Received request: {request.request}")
        if request.request == "GETLEADER":
            return raft_pb2.ServeClientResponse(data="NA", leader_id=self.leader_id, success=True)
        
        elif request.request.split()[0] == "GET":
            key = request.request.split()[1]
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


if __name__ == '__main__':
    run_server()
