import grpc
import raft_pb2
import raft_pb2_grpc
import threading


class RaftClient:
    def __init__(self, node_addresses) -> None:
        self.leader_id = -1
        self.leader_found = False
        self.node_addresses = node_addresses
        self.verbose = True
    
    
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
                if self.verbose:
                    print("Error in checking leader status of node:", id)
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
                if self.verbose:
                    print("Error in sending request to node:", id)
                return False, None
    
    
    def find_leader(self, request="GETLEADER"):
        try:
            if self.leader_id not in self.node_addresses.keys():       # Client does not have the leader
                for id, address in self.node_addresses.items():
                    
                    success, response = self.__check_leader_status__(id, address, request)
                    if success:
                        self.leader_id = response.leader_id
                        self.leader_found = True
                        return 1
                
                if self.leader_found == False:      # Raft does not have a leader
                    return -1
            
            else:
                success, response = self.__check_leader_status__(self.leader_id, self.node_addresses[self.leader_id], request)
                if success:
                    if self.leader_id != response.leader_id:        # Leader changed
                        self.leader_id = response.leader_id
                        self.leader_found = True
                    else:
                        self.leader_found = True                 # Leader is same
                    return 1
                else:
                    self.leader_id = -1         # Client's POV leader is down
                    self.leader_found = False
                    return self.find_leader()

        except Exception as e:
            if self.verbose:
                print("Error in find_leader method:", e)
            return -1


    def get(self, key):
        try:
            status = self.find_leader()
            if status == -1: #not self.leader_found:
                print("Leader not present in Raft!")
                return -1
            
            request = f"GET {key}"
            _, response = self.__send_request__(self.leader_id, self.node_addresses[self.leader_id], request)
            if response.success:
                print(f"{key} = {response.data}")
            else:
                print("Key could not be retrieved!")
            return 1
        
        except Exception as e:
            print("Error in get method:", e)
            return -1


    def set(self, key, value):
        try:
            status = self.find_leader()
            if status == -1: #self.leader_id == -1 or not self.leader_found:
                print("Leader not present in Raft!")
                return -1
            
            request = f"SET {key} {value}"
            _, response = self.__send_request__(self.leader_id, self.node_addresses[self.leader_id], request)
            print(response.data)
            return 1
        
        except Exception as e:
            print("Error in set method:", e)
            return -1


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
            client.verbose = False
            status = client.find_leader()
            if status == -1:
                print("Leader not present in Raft!")
            else:
                print(f"Leader found at node: {client.leader_id}")
            client.verbose = True

        elif choice == 1:
            key = input("Enter the key: ")
            client.verbose = False
            client.get(key)
            client.verbose = True
        
        elif choice == 2:
            key = input("Enter the key: ")
            value = input("Enter the value: ")
            client.verbose = False
            client.set(key, value)
            client.verbose = True
        
        else:
            print("Invalid choice!")
        choice = main_menu()


if __name__ == '__main__':
    try:
        serve()
    except Exception as e:
        print("\nClient stopped!", e)