import grpc
import raft_pb2
import raft_pb2_grpc
import threading


class RaftClient:
    def __init__(self, node_addresses) -> None:
        self.leader_id = -1
        self.leader_found = False
        self.node_addresses = node_addresses
    
    
    def __check_leader_status__(self, id, address, request):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            try:
                response = stub.ServeClient(raft_pb2.ServeClientRequest(request=request))
                if response.leader_id >= 0:     # Leader found in this node
                    return True, response
                else:
                    return False, response
            except grpc.RpcError as e:
                print(e)
                return False, None
    
    
    def __send_request__(self, id, address, request):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            try:
                response = stub.ServeClient(raft_pb2.ServeClientRequest(request=request))
                if response.success:
                    return True, response
                else:
                    return False, response
            except grpc.RpcError as e:
                print(e)
                return False, None
    
    
    def find_leader(self, request="GETLEADER"):
        if self.leader_id not in self.node_addresses.keys():       # Client does not have the leader
            for id, address in self.node_addresses.items():
                
                success, response = self.__check_leader_status__(id, address, request)
                if success:
                    self.leader_id = response.leader_id
                    self.leader_found = True
                    print("Leader:", self.leader_id)
                    return
            
            if self.leader_found == False:      # Raft does not have a leader
                print("Leader not present in Raft!")
        
        else:
            success, response = self.__check_leader_status__(self.leader_id, self.node_addresses[self.leader_id], request)
            if success:
                if self.leader_id != response.leader_id:        # Leader changed
                    self.leader_id = response.leader_id
                    self.leader_found = True
            else:
                self.leader_id = -1         # Client's POV leader is down
                self.leader_found = False
                self.find_leader()                   


    def get(self, key):
        # self.find_leader()
        # print("Leader:", self.leader_id)
        if not self.leader_found:
            return
        
        request = f"GET {key}"
        # print(self.node_addresses)
        _, response = self.__send_request__(self.leader_id, self.node_addresses[self.leader_id], request)
        print(f"{key} = {response.data}")


    def set(self, key, value):
        self.find_leader()
        print("Leader:", self.leader_id)
        if not self.leader_found:
            return
        
        request = f"SET {key} {value}"
        _, response = self.__send_request__(self.leader_id, self.node_addresses[self.leader_id], request)
        print(response.data)


def main_menu():
    print('-' * 25)
    print("0. GetLeader")
    print("1. GET <key>")
    print("2. SET <key> <value>")
    print("3. EXIT")
    choice = int(input("Enter your choice: "))
    print()
    return choice


def fetch_server_address():
    node_addresses = {}
    with open('Config.conf') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.strip().split()
                id, address, port = parts[0], parts[1], parts[2]
                node_addresses[int(id)] = f'{str(address)}:{str(port)}'

    return node_addresses


def serve():
    node_addresses = fetch_server_address()

    client = RaftClient(node_addresses)

    print('-' * 25)
    print("Client started!")
    choice = main_menu()

    while choice != 3:
        if choice == 0:
            client.find_leader()

        elif choice == 1:
            key = input("Enter the key: ")
            client.get(key)
        
        elif choice == 2:
            key = input("Enter the key: ")
            value = input("Enter the value: ")
            client.set(key, value)
        
        else:
            print("Invalid choice!")
        choice = main_menu()


if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        print("\nClient stopped!")