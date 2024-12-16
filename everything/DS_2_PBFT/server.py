import argparse
import grpc
from concurrent import futures
import asyncio
import sys
import pbft_pb2
import pbft_pb2_grpc
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
import threading
import hashlib
import base64
from genKeyPair import sign_message
import ast

def load_public_key_client(client):
    with open(f"./keys/client_keys/client_{client}_public_key.pem", "rb") as f:
        return serialization.load_pem_public_key(f.read())
def load_public_key_server(server):
    with open(f"./keys/server_keys/public_key_{server}.pem", "rb") as f:
        return serialization.load_pem_public_key(f.read())

class PBFTServiceServicer(pbft_pb2_grpc.PBFTServiceServicer):
    def __init__(self, port):
        self.port = str(port)
        self.request_count = 0
        self.sequence_number = 0  
        self._sequence_lock = threading.Lock()
        self.view_number = 1
        self.vote_count = {}
        self.transaction_list = {}
        self.request_block = {}
        self.flag1 = {}
        self.vote_count_commit = {}
        self.request_block_commit = {}
        self.flag2 = {}
        self.balances = {'A':10,'B':10,'C':10,'D':10,'E':10,
        'F':10,'G':10,'H':10,'I':10,'J':10}    
        self.timer_task = None
        self.TIMEOUT_SECONDS = 35
        self.view_changing = False
        self.id = int(self.port[5])
        #self.view_timer_task = None
        #self.VIEW_TIMEOUT_SECONDS = 5
        self.view_change_vote_count = 1
        self.view_change_request_block = []
        self.flag_8 = 0
        #self.view_timer_task = None
        #self.VIEW_TIMEOUT_SECONDS = 5
        self.log = ""
        self.view_logs = ""
    def increment_sequence_number(self):
        with self._sequence_lock:
            self.sequence_number += 1
            return self.sequence_number

    def collision_resistant_hash(self, input_string):
        if not isinstance(input_string, bytes):
            input_string = input_string.encode('utf-8')
        sha256_hash = hashlib.sha256()
        sha256_hash.update(input_string)
        hash_digest = sha256_hash.digest()
        base64_hash = base64.b64encode(hash_digest).decode('utf-8')
        return base64_hash
    def verify_signature(self,public_key,request,context):
        try:
                public_key.verify(
                    request.signature,
                    request.name.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
        except Exception as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Signature verification failed: {str(e)}")
                raise
    # async def start_view_change_timer(self, server_list,byzantine_list):
    #     if self.view_timer_task is not None:
    #         print("View Change Timer restarted")
    #         self.view_timer_task.cancel()
    #     try:
    #         self.view_timer_task = asyncio.create_task(self.run_view_change_timer(server_list,byzantine_list))
    #     except Exception as e:
    #         print(f"Error starting view change timer: {e}")
    # async def run_view_change_timer(self, server_list,byzantine_list):
    #     try:
    #         await asyncio.sleep(self.VIEW_TIMEOUT_SECONDS)
    # 
    #         print(f"View Change Timer expired for server {self.port}")
    #         await self.timer_expired_vc(server_list,byzantine_list)
    #     except asyncio.CancelledError:
    #         print("View change timer cancelled")
    #         pass
    #     except Exception as e:
    #         print(f"View change timer error: {e}")
    # async def stop_view_change_timer(self):
    #     if self.view_timer_task is not None:
    #         self.view_timer_task.cancel()
    #         self.view_timer_task = None
    async def PrintStatus(self, request, context):
        return pbft_pb2.PrintStatusResponse(
             status = str(self.transaction_list[request.sequence_number])+"on server"+str(self.port)
        )
    async def PrintView(self,request,context):
        return pbft_pb2.PrintViewResponse(view_logs=self.view_logs)
    async def PrintLog(self, request, context):
        return pbft_pb2.PrintLogResponse(log=self.log)
    async def ChangeServerTimeout(self, request, context):
        print("WHOOP")
        self.TIMEOUT_SECONDS = 34
        return pbft_pb2.ChangeServerTimeoutResponse()
    async def timer_expired(self,server_list,byzantine_list):
        print(f"TMR Timer expired for server {self.port}")
        print("Starting view change")
        self.view_number += 1
        self.view_changing = True
        #await self.start_view_change_timer(server_list,byzantine_list)
        tasks = [
                self.send_view_change(server,self.view_number,server_list,byzantine_list) 
             for server in server_list if server!= str(self.port)
            ]
        result = await asyncio.gather(*[
        asyncio.create_task(task) for task in tasks
            ])
        print(f"Timer expired result: {result}")
        return False  
    async def timer_expired_vc(self,server_list,byzantine_list):
       # await self.stop_view_change_timer()
        print(f"VC TMR Timer expired for server {self.port}")
        print("Starting view change")
        self.view_number += 2
        self.view_changing = True
        #await self.start_view_change_timer(server_list,byzantine_list)
        tasks = [
                self.send_view_change(server,self.view_number,server_list,byzantine_list) 
             for server in server_list if server!= str(self.port)
            ]
        result = await asyncio.gather(*[
        asyncio.create_task(task) for task in tasks
            ])
        print(f"Timer expired result: {result}")
        return False  
    async def send_view_change(self,server,view_number,server_list,byzantine_list):
       
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
                
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
               # print(f"I am {self.port} sending view change request to server{server} with view number{request.view_number}")
                try:
                    #print(f"Server: {server}")
                    signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(self.port)}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(self.port)}:{sign_error}")
                    return None
                request = pbft_pb2.ViewChangeRequest (
                    view_number = view_number,
                    name = str(self.port),
                    signature=signature,
                    server_list = server_list,
                    byzantine_list = byzantine_list
                )
                try:
                    response = await stub.ViewChange(request)                    
                    return pbft_pb2.ViewChangeResponse(response=True)
                except grpc.RpcError as e:
                    print(f"RPC failed for {server} with {e.code()}: {e.details()}")
                    return None
    
    async def ViewChange(self,request,context):
        try:
            public_key_server = load_public_key_server(request.name)
            self.verify_signature(public_key_server,request,context)
            print(f"Received View Change from {request.name} with view number {request.view_number}")
            self.view_change_vote_count+=1
            if request.view_number%7 == self.id:
                #await self.stop_view_change_timer()
                self.view_number = request.view_number
                self.view_changing = False
                await asyncio.sleep(0.5)
                self.view_change_request_block.append(request)
                if self.view_change_vote_count>=5 and self.flag_8==0:
                    self.flag_8=1
                    self.view_number = request.view_number
                    self.view_changing = False
                    tasks = [
                        self.sendNewView(server,self.view_number,request.byzantine_list) 
                        for server in request.server_list if server!= str(self.port)
                    ]
                    result = await asyncio.gather(*[   
                        asyncio.create_task(task) for task in tasks
                    ])
                    return pbft_pb2.ViewChangeResponse(response=True)
            return pbft_pb2.ViewChangeResponse(response=True)
        except Exception as e:
            # Handle errors
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Internal error: {str(e)}')
            raise
    async def sendNewView(self,server,view_number,byzantine_list):
        
        if self.port in byzantine_list:
            await asyncio.sleep(6)
            return pbft_pb2.NewViewResponse(response=False)
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
                
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                print(f"Sending new view to server{server}")
                try:
                    signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(self.port)}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(self.port)}: {sign_error}")
                    return None
                request = pbft_pb2.NewViewRequest(
                    view_number = view_number,
                    name = str(self.port),
                    signature=signature,
                    view_certificate = self.view_change_request_block,
                    byzantine_list = byzantine_list
                )
                try:
                    response = await stub.NewView(request)                    
                    return pbft_pb2.NewViewResponse(response=True)
                except grpc.RpcError as e:
                    print(f"RPC failed for {server} with {e.code()}: {e.details()}")
                    return None
    async def NewView(self,request,context):
        #await self.stop_view_change_timer()
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        print(f"Received New View from {request.name} with view number {request.view_number}")
        self.view_number = request.view_number
        self.view_changing = False
        self.view_logs += str(request)
        return pbft_pb2.NewViewResponse(response=True)

    async def start_timer(self,server_list,byzantine_list):

        if self.timer_task is not None:
            print("TMR Timer restarted")
            self.timer_task.cancel()
        try:
            self.timer_task = asyncio.create_task(self.run_timer(server_list,
                                                                 byzantine_list))
        except Exception as e:
            print(f"Error starting timer: {e}")

    async def run_timer(self,server_list,byzantine_list):
        try:
            await asyncio.sleep(self.TIMEOUT_SECONDS)
            await self.timer_expired(server_list,byzantine_list)
        except asyncio.CancelledError:
            # Timer was cancelled, do cleanup if needed
            pass
        except Exception as e:
            print(f"Timer error: {e}")

    async def stop_timer(self):
        if self.timer_task is not None:
            self.timer_task.cancel()
            self.timer_task = None
        
    async def ClientTransaction(self, request, context):
        self.log += str((request.message,
                    self.sequence_number,self.view_number))
        if self.view_changing:
            return pbft_pb2.ClientTransactionResponse(response=False)
        if request.view_number%7 != self.id:
            return pbft_pb2.ClientTransactionResponse(response=False)
        try:
           
            print(f"Received transaction from client {request.message}")
            public_key = load_public_key_client(request.name)
            self.verify_signature(public_key,request,context)
            self.sequence_number = self.increment_sequence_number()
            self.flag1[self.sequence_number] = 0
            self.flag2[self.sequence_number] = 0
            digest = self.collision_resistant_hash(request.message)
            l = []
            l.append(list(ast.literal_eval(request.message)))
            l.append(request.timestamp)
            l.append("NA")
            l.append(digest)
            self.transaction_list[self.sequence_number] = l
            
            response = await self.sendPrePrepare(request.server_list,self.view_number,request,request.byzantine_list,digest)
            print(f"L1 {response}")
            if request.view_number != self.view_number:
                return pbft_pb2.ClientTransactionResponse(response=False)
            return pbft_pb2.ClientTransactionResponse(response=response)
            
        except Exception as e:
            # Handle errors
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Internal error: {str(e)}')
            raise 
    async def sendPrePrepare(self,server_list,view_number,request,byzantine_list,digest):
        if self.view_changing:
            return 
        tasks = [
                self.PrePrepare(server,digest,self.sequence_number,self.view_number, request,server_list,byzantine_list) 
             for server in server_list if server!= str(self.port)
            ]
        result = await asyncio.gather(*[
        asyncio.create_task(task) for task in tasks
            ])
        print(f"L2 {result}")
        return False not in result
    async def PrePrepare(self,server,digest,sequence_number,view_number,client_request,server_list,byzantine_list):
        if self.view_changing:
            return pbft_pb2.PrePrepareResponse(response=False)
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
                
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                print(f"In PrePrepare Phase, sending PrePrepare to server{server}")
                try:
                    signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(self.port)}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(self.port)}: {sign_error}")
                    return None
                request = pbft_pb2.PrePrepareRequest(
                    digest = digest,
                    sequence_number = sequence_number,
                    view_number = view_number,
                    signature=signature,
                    client_request = client_request,
                    name = str(self.port),
                    server_list = server_list,
                    byzantine_list = byzantine_list
                )
                try:
                    response = await stub.ReplicaPrePrepare(request)
                    print(f"L3 {response}")                 
                    return pbft_pb2.PrePrepareResponse(response=True)
                
                except grpc.RpcError as e:
                    print(f"RPC failed for preprep {server} with {e.code()}: {e.details()}")
                    return None
    async def ReplicaPrePrepare(self,request,context):
        if self.view_changing:
            return pbft_pb2.PrePrepareResponse(response=False)
        try:
            self.log += str((request.client_request.message,
                    self.sequence_number,self.view_number))
            await self.start_timer(request.server_list,request.byzantine_list)
            print(f"Timer started for server {self.port}")
            #self.flag1[request.sequence_number]==0
            #self.flag2[request.sequence_number]==0
            public_key_server = load_public_key_server(request.name)
            public_key_client = load_public_key_client(request.client_request.name)
            try:
                print(f"Received PrePrepare from {request.name} with sequence number {request.sequence_number}")
                self.verify_signature(public_key_client,request.client_request,context)
                self.verify_signature(public_key_server,request,context)
                digest = request.digest
                sequence_number = request.sequence_number
                view_number = request.view_number
                if (digest == self.collision_resistant_hash(
                    request.client_request.message)):
                    try:
                        l = []
                        l.append(list(ast.literal_eval(request.client_request.message)))
                        l.append(request.client_request.timestamp)
                        l.append("NA")
                        l.append(digest)
                        self.transaction_list[request.sequence_number] = l
                        signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
                        result = await self.sendPrepare(digest,view_number,signature,sequence_number,request.name,request.server_list, request.byzantine_list)
                        print(f"L4 {result}")
                        await self.stop_timer()
                        return pbft_pb2.PrePrepareResponse(response=True)
                    except FileNotFoundError:
                        print(f"Error: Private key file not found for {str(self.port)}")
                        return None
                    except Exception as sign_error:
                        print(f"Signature creation error for {str(self.port)}: {sign_error}")
                        return None
                
                
                
            except Exception as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Signature verification failed: {str(e)}")
                raise
        except Exception as e:
            # Handle errors
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Internal error: {str(e)}')
            raise
    async def sendPrepare(self,digest,view_number,signature,sequence_number,name,server_list,byzantine_list):
        if self.view_changing:
            return
        async with grpc.aio.insecure_channel("localhost:"+name) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            print(f"In Prepare Phase, sending Prepare to server{name}")
            request = pbft_pb2.PrepareRequest(
                        digest = digest,
                        sequence_number = sequence_number,
                        view_number = view_number,
                        signature=signature,
                        name = str(self.port),
                        server_list = server_list,
                        byzantine_list = byzantine_list
                    )
            try:
                response = await stub.Prepare(request)
                print(f"L5 {response}")                    
                #return response.response
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
                
    async def Prepare(self,request,context):
        if self.view_changing:
            return pbft_pb2.PrepareResponse(response=False)
        view = request.view_number
        if self.port in request.byzantine_list:
            print("Byzantine leader alert")
            while(self.view_number == view):
                #print("AAAAAAAA")
                await asyncio.sleep(0.1)
            print(f"L6 False")
            return pbft_pb2.PrepareResponse(response=False)
        print(f"I the primary server {self.port} received prepare request from {request.name} with sequence number {request.sequence_number}")
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        if request.sequence_number not in self.vote_count:
            self.vote_count[request.sequence_number] = 2
        else:
            self.vote_count[request.sequence_number] += 1
        if request.digest != self.transaction_list[
            request.sequence_number][3]:
            return pbft_pb2.PrepareResponse(response=False)
        if request.sequence_number not in self.request_block:
            a = []
            a.append(request)
            self.request_block[request.sequence_number] = a
        else:
            self.request_block[request.sequence_number].append(request)
        await asyncio.sleep(0.1)
        print("Phase 3 sucessful")
        if (self.vote_count[request.sequence_number]>=5) and self.flag1[request.sequence_number]==0:
            self.flag1[request.sequence_number]=1
            server_list = request.server_list
            digest = request.digest
            self.transaction_list[request.sequence_number][2] = "PREPARED"
            tasks = [
                self.sendReady(server,self.request_block[request.sequence_number],request.sequence_number,digest,server_list,request.byzantine_list) 
             for server in server_list if server!= str(self.port)
            ]
            result = await asyncio.gather(*[
            asyncio.create_task(task) for task in tasks
                ])
            return pbft_pb2.PrepareResponse(response=True)
        
        return pbft_pb2.PrepareResponse(response=True)

    async def sendReady(self,server,request_block,sequence_number,digest,server_list,byzantine_list):
        if self.view_changing:
            return
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            print(f"In Ready Phase, sending Ready to serverzz {server} with sequence number {sequence_number}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))                   
            request = pbft_pb2.PrepareBroadcastRequest(
                            view_number = self.view_number,
                            sequence_number = sequence_number,
                            prepare_requests_block = request_block,
                            name = str(self.port),
                            signature=signature,
                            digest = digest,
                            server_list = server_list,
                            byzantine_list = byzantine_list
            )

            try:
                response = await stub.PrepareBroadcast(request)  
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def PrepareBroadcast(self, request, context):
        if self.view_changing:
            return pbft_pb2.PrepareBroadcastResponse(response=False)
        print(f"Received Prepare Broadcast from {request.name}")
        print(f"The length of request block is {len(request.prepare_requests_block)}")          
        digest = request.digest
        print(f"Prepare Broadcast recieved by {self.port}")
        print(len(request.prepare_requests_block))
        request_block = request.prepare_requests_block
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        if 0==0:
            print(self.port not in request.byzantine_list)
            if self.port not in request.byzantine_list:
                self.transaction_list[request.sequence_number][2] = "PREPARED"
        
                await self.sendCommit(request.name,request.sequence_number,request.digest,request.server_list,request.byzantine_list)
                return pbft_pb2.PrepareBroadcastResponse(response=True)
        return pbft_pb2.PrepareBroadcastResponse(response=True)
    async def sendCommit(self,name,sequence_number,digest,server_list,byzantine_list):
        if self.view_changing:
            return None
        async with grpc.aio.insecure_channel("localhost:"+name) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            print(f"In Commit Phase, sending Commit to server{name}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
            request = pbft_pb2.CommitRequest(
                        sequence_number = sequence_number,
                        digest = digest,
                        name = str(self.port),
                        signature=signature,
                        server_list = server_list,
                        byzantine_list = byzantine_list
                    )
            try:
                response = await stub.Commit(request)                    
                return response
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def Commit(self,request,context):
        if self.view_changing:
            return pbft_pb2.CommitResponse(response=False)
        print(f"Received Commit req from {request.name} with sequence number {request.sequence_number}")
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        if request.sequence_number not in self.vote_count_commit:
            self.vote_count_commit[request.sequence_number] = 2
        else:
            self.vote_count_commit[request.sequence_number] += 1
        if request.sequence_number not in self.request_block_commit:
            a = []
            a.append(request)
            self.request_block_commit[request.sequence_number] = a
        else:
            self.request_block_commit[request.sequence_number].append(request)
        await asyncio.sleep(0.1)
        if self.vote_count_commit[request.sequence_number]>=5 and self.flag2[request.sequence_number]==0:
            self.flag2[request.sequence_number]=1
            print("Request Block Commit")
            print(len(self.request_block_commit))
            if self.transaction_list[request.sequence_number][2] == "PREPARED":
                self.transaction_list[request.sequence_number][2] = "COMMITTED"
                print("Final Transaction List")
                print(self.transaction_list)
                self.transaction_list = {key: self.transaction_list[key] for key in sorted(self.transaction_list)}
                flag3 = 0
                flag5 = 0
                for key in self.transaction_list:
                    if self.transaction_list[key][2] != "COMMITTED":
                        flag5 = 1
                if 0 == 0:
                    
                    await asyncio.sleep(0.2)
                    for seq in self.transaction_list:
                        if self.transaction_list[seq][2] == "COMMITTED":
                            self.transaction_list[seq][2] = "EXECUTED"
                            x = self.transaction_list[seq][0][0]
                            y = self.transaction_list[seq][0][1]
                            amt = self.transaction_list[seq][0][2]
                            if self.balances[x] - amt >= 0:
                                print(f"Executing {x}-{amt} and {y}+{amt}")
                                self.balances[x] = self.balances[x] - amt
                                self.balances[y] = self.balances[y] + amt
                        else:
                            flag3 = 1
                server_list = request.server_list
                tasks = [
                    self.sendCommitBroadcast(server,self.request_block_commit[request.sequence_number],request.sequence_number,request.digest,server_list,request.byzantine_list) 
                for server in server_list if server!= str(self.port)
                ]
                result = await asyncio.gather(*[
                asyncio.create_task(task) for task in tasks
                    ])
                print("Final Balances")
                print(self.balances)
                if flag3 == 1:
                    return pbft_pb2.CommitResponse(response=False)
        
        return pbft_pb2.CommitResponse(response=True)
    async def sendCommitBroadcast(self,server,request_block_commit,sequence_number,digest,server_list,byzantine_list):
        if self.view_changing:
            return None
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            print(f"In Commit Broadcast Phase, sending Commit Broadcast to server{server} with sequence number {sequence_number}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
            request = pbft_pb2.CommitBroadcastRequest(
                        sequence_number = sequence_number,
                        digest = digest,
                        name = str(self.port),
                        signature=signature,
                        commit_requests_block = request_block_commit,
                        server_list = server_list,
                        byzantine_list = byzantine_list
                    )
            try:
                response = await stub.CommitBroadcast(request)                    
                return response
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def CommitBroadcast(self,request,context):
        if self.view_changing:
            return pbft_pb2.CommitBroadcastResponse(response=False)
        print(f"In Commit Broadcast Phase, received Commit Broadcast from server{request.name} with sequence number {request.sequence_number}")
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        digest = request.digest
        print(len(request.commit_requests_block))
        request_block_commit = request.commit_requests_block
        print(f"Request Block Commit length is {len(request_block_commit)}")
        if 0==0:
            print("in")
            print(self.transaction_list)
            if self.transaction_list[request.sequence_number][2] == "PREPARED":
                self.transaction_list[request.sequence_number][2] = "COMMITTED"
                print("Final Transaction List")
                print(self.transaction_list)
                self.transaction_list = {key: self.transaction_list[key] for key in sorted(self.transaction_list)}
                flag4 = 0
                flag6 = 0
                for key in self.transaction_list:
                    if self.transaction_list[key][2] != "COMMITTED":
                        flag6 = 1
                if 0 == 0:
                    print("Dekh le bhai")
                    await asyncio.sleep(0.2)
                    for seq in self.transaction_list:
                        if self.transaction_list[seq][2] == "COMMITTED":
                            self.transaction_list[seq][2] = "EXECUTED"
                            x = self.transaction_list[seq][0][0]
                            y = self.transaction_list[seq][0][1]
                            amt = self.transaction_list[seq][0][2]
                            if self.balances[x] - amt >= 0:
                                print(f"Executing {x}-{amt} and {y}+{amt}")
                                self.balances[x] = self.balances[x] - amt
                                self.balances[y] = self.balances[y] + amt
                            else:
                                flag4 = 1
                print(f"Final Balances->{request.sequence_number}")
                print(self.balances)
                if flag4 == 0:
                    return pbft_pb2.CommitBroadcastResponse(response=True)
        return pbft_pb2.CommitBroadcastResponse(response=False)
    def ResetServerState(self, request, context):
        try:
            self.request_count = 0
            self.sequence_number = 0
            self.view_number = 1
            self.vote_count = {}
            self.transaction_list = {}
            self.request_block = {}
            self.flag1 = {}
            self.vote_count_commit = {}
            self.request_block_commit = {}
            self.flag2 = {}
            self.timer_task = None
            self.TIMEOUT_SECONDS = 25
            self.view_changing = False
            self.id = int(self.port[5])
            # self.view_timer_task = None
            # self.VIEW_TIMEOUT_SECONDS = 10
            self.view_change_vote_count = 1
            self.view_change_request_block = []
            self.flag_8 = 0
            self.log = ""
            self.view_logs = ""
            # Reset balances to initial state
            self.balances = {
                'A':10, 'B':10, 'C':10, 'D':10, 'E':10,
                'F':10, 'G':10, 'H':10, 'I':10, 'J':10
            }

            print(f"Server {self.port} state reset successfully")

            return pbft_pb2.ResetServerStateResponse(
                reset_successful=True,
                server_port=str(self.port)
            )
        
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error resetting server state: {str(e)}')
            raise
    def GetServerBalances(self, request, context):
        try:
            balances_map = pbft_pb2.BalancesResponse()
            for key, value in self.balances.items():
                balances_map.balances[key] = value

            print(f"Server {self.port} balances requested and returned")

            return balances_map
        
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Error retrieving balances: {str(e)}')
            raise
class PBFTServer:
    def __init__(self, port):
        self.port = port
        
        # Create gRPC server
        self.grpc_server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        
        # Create servicer
        self.servicer = PBFTServiceServicer(self.port)

    async def start(self):
        try:
            # Add servicer to server
            pbft_pb2_grpc.add_PBFTServiceServicer_to_server(
                self.servicer, 
                self.grpc_server
            )

            # Bind to localhost and specified port
            self.grpc_server.add_insecure_port(f'localhost:{self.port}')
            
            # Start server
            await self.grpc_server.start()
            print(f"PBFT Server started on localhost port {self.port}")
            
            # Keep server running
            await self.grpc_server.wait_for_termination()
        
        except Exception as e:
            print(f"Server startup failed: {e}")
            sys.exit(1)

    async def stop(self):
        """
        Gracefully stop the server
        """
        await self.grpc_server.stop(0)
        print(f"Server on port {self.port} stopped.")

# Main Execution
async def main():
    parser = argparse.ArgumentParser(description='PBFT Server')
    parser.add_argument('port', type=int, help='Port number for the server')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create and start server
    server = PBFTServer(args.port)
    await server.start()

if __name__ == '__main__':
    asyncio.run(main())