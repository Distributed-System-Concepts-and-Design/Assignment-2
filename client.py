import grpc
import raft_pb2
import raft_pb2_grpc
import threading


class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leader_id = -1
    

    def find_leader(self):
        request = "GETLEADER"
        # _, _, success = self.send_request_to_leader(request)
        success, _ = self.send_request(request)
        if not success:
            print("Error: Could not get leader")
        else:
            print(f"Leader ID: {self.leader_id}")


    def __send_request__(self, id, address, request):
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                response = stub.ServeClient(raft_pb2.ServeClientRequest(request=request))
                if response.success:
                    self.leader_id = response.leader_id
                    return True, response
                else:
                    print("Failed")
                    return False, None

        except grpc.RpcError as e:
            print(e.details())
            return False, None
    

    def send_request(self, request):
        for id, address in self.node_addresses.items():
            success, response = self.__send_request__(id, address, request)
            if success:
                return True, response
        self.leader_id = -1
        return False, response


    # def send_request_to_leader(self, request):
    #     # print("*"*50, self.leader_id)
    #     if self.leader_id >= 0:
    #         # Connect to the leader address directly
    #         leader_address = self.node_addresses[self.leader_id]
    #         with grpc.insecure_channel(leader_address) as channel:
    #             stub = raft_pb2_grpc.RaftNodeStub(channel)
    #             try:
    #                 response = stub.ServeClient(raft_pb2.ServeClientRequest(request=request))
    #                 if response.leader_id != self.leader_id:
    #                     self.leader_id = response.leader_id
    #                 print("response:", response)
    #                 return response.data, response.leader_id, response.success
    #             except grpc.RpcError as e:
    #                 print(f"Error: {e}")
    #                 return None, None, False
    #     else:
    #         # Connect to all nodes in parallel and get the leader
    #         threads = []
    #         responses = []
    #         for id, address in self.node_addresses.items():                
    #             success, led = self.send_request(id, address, request)
    #             # print(f"###Sent request to node {id}", led, success)
    #             if success:
    #                 return None, None, True

    #             # thread = threading.Thread(target=send_request, args=(id, address, request))
    #             # threads.append(thread)
    #             # thread.start()


    #         # Break the threads if the leader is found
    #         # for thread in threads:
    #         #     thread.join()
    #         if self.leader_id != -1:
    #             return None, None, False
    #         else:
    #             return None, None, True


    def get(self, key):
        request = f"GET {key}"
        _, response = self.send_request(request)
        print(f"{key} = {response.data}")


    def set(self, key, value):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
        request = f"SET {key} {value}"
        _, response = self.send_request(request)
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
                node_addresses[id] = f'{str(address)}:{str(port)}'

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
    serve()