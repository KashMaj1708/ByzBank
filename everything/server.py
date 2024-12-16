import asyncio
import sys
import grpc
#import transactions_pb2
#import transactions_pb2_grpc
from tinydb import TinyDB, Query
from cryptography.hazmat.primitives import serialization 
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from genKeyPair import sign_message
import hashlib
import threading
import base64
#import project4_pb2
#import project4_pb2_grpc
import ast
import project4_pb2
import project4_pb2_grpc
from collections import deque

def load_public_key_client(client):
    with open(f"./keys/client_keys/client_{client}_public_key.pem", "rb") as f:
        return serialization.load_pem_public_key(f.read())
def load_public_key_server(server):
    with open(f"./keys/server_keys/public_key_{server}.pem", "rb") as f:
        return serialization.load_pem_public_key(f.read())

class Server(project4_pb2_grpc.project4ServiceServicer):
    def __init__(self, name, port):
        self.name = name
        self.port = str(port)
        self.db = TinyDB(f'client_balances_{self.port}.json')
        self.initialize_client_balances()
        self.view_number = 0
        self.log = ""
        self._sequence_lock = threading.Lock()
        self.sequence_number = 0
        self.flag1 = {}
        self.flag2 = {}
        self.transaction_list = {}
        self.request_block = {}
        self.vote_count_commit = {}
        self.request_block_commit = {}
        self.vote_count = {}
        self.locks = {}
        self.pending_queue = {}
        self.pending_transactions = deque()
        self.pending_transactions_cross_shard = deque()
        #self.contact_servers = ["500051","500054","500057"]
        #self.server_list_clustered = [["500051","500052","500053"],["500054","500055","500056"],["500057","500058","500059"]]
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
    def initialize_client_balances(self):
        if self.port in ['500051', '500052', '500053','500054']:
            client_range = range(1, 1000)
        elif self.port in ['500055', '500056', '500057','500058']:
            client_range = range(1001, 2000)
        elif self.port in ['500059', '500060', '500061','500062']:
            client_range = range(2001, 3000)
        else:
            client_range = []
        for client_id in client_range:
            try:
                self.db.insert({'client_id': client_id, 'balance': 10})
            except Exception as e:
                print(e)
    async def GetBalance(self,request,context):
        ClientQuery = Query()
        client_balance = self.db.get(ClientQuery.client_id == request.client_id)
        return project4_pb2.GetBalanceResponse(balance= str(client_balance['balance']) if client_balance else "NA")
    async def ClientTransactionCrossShard (self,request,context):
        try:
            print("Executing Cross Shard Transaction")
            if self.pending_transactions_cross_shard and not request.isPendingTrans:
                return project4_pb2.ClientTransactionCrossShardResponse(response=False)
                # while self.pending_transactions_cross_shard:
                #     self.sequence_number, pending_request = self.pending_transactions_cross_shard.popleft()
                #     print("pending request")
                #     print(pending_request)
                #     print("current request")
                #     print(request)
                #     #pending_request.server_list = request.server_list
                #     try:
                #         new_request = project4_pb2.ClientTransactionCrossShardRequest(
                #             name=pending_request.name,
                #             message=pending_request.message,
                #             timestamp=pending_request.timestamp,
                #             signature=pending_request.signature,
                #             server_list_1 = request.server_list_1,
                #             server_list_2 = request.server_list_2,
                #             byzantine_list = request.byzantine_list,
                #             isIntraShard = pending_request.isIntraShard,
                #             #isRollBack = pending_request.isRollBack,
                #             signature_1 = pending_request.signature_1,
                #             signature_2 = pending_request.signature_2,
                #             contact_server_list = request.contact_server_list,
                #             isPendingTrans = True
                #         )
                #         print("New Request")
                #         print(new_request)
                #         async with grpc.aio.insecure_channel("localhost:"+self.port) as channel:
                #             stub = project4_pb2_grpc.project4ServiceStub(channel)
                #             response = await stub.ClientTransactionCrossShard(new_request)
                #             print("Sent pending transaction")
                #             print(new_request.message)

                #             print(response)
                #             #print(request)
                #     except Exception as e:
                #         print(e)
            
            #return project4_pb2.ClientTransactionCrossShardResponse(response=True)
            cleaned = request.message.strip("()").replace("'", "").split(",")
            cleaned = [x.strip(" ") for x in cleaned]
            print(f"Cleaned = {cleaned}")  
            TwoPhase = {}
            transactions = request.message
            timestamp = request.timestamp
            byzantine_list = request.byzantine_list
            server_list_1 = request.server_list_1
            server_list_2 = request.server_list_2
            signature = request.signature
            contact_server_list = request.contact_server_list
            signature_1 = request.signature_1
            signature_2 = request.signature_2 
            message = request.message 
            a = int(cleaned[0])//1000
            b = int(cleaned[1])//1000
            print(f" A-> {request.contact_server_list[a]} B-> {request.contact_server_list[b]}")
            print(f"Server LISTS, server_list_1 = {request.server_list_1} server_list_2 = {request.server_list_2}")
            r = request
            request = project4_pb2.ClientTransactionRequest(
                        name=cleaned[0],
                        message=request.message,
                        timestamp=timestamp,
                        signature=signature_1,
                        server_list = server_list_1,
                        byzantine_list = list(byzantine_list),
                        isIntraShard = False,
                        cross_shard_request = r
                        #view_number = view_number
                    )
            #print(request)
            print("Ultra A")
            print(request)
            async with grpc.aio.insecure_channel("localhost:"+str(contact_server_list[a])) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    try:
                        response = await stub.ClientTransaction(request) 
                        print("Done with first phase")
                        print(response)
                    except Exception as e:
                        print(e)
            TwoPhase[request.message] = response.response
            print("Ultra B")
            print(b)
            if response.response == "ABORTED":
                print("Failed in the first phase")
                return project4_pb2.ClientTransactionCrossShardResponse(response=False)
            request = project4_pb2.ClientTransactionRequest(
                        name=cleaned[1],
                        message=request.message,
                        timestamp=timestamp,
                        signature=signature_2,
                        server_list = server_list_2,
                        byzantine_list = list(byzantine_list),
                        isIntraShard = False,
                        cross_shard_request = r
                        #view_number = view_number
                    )
            async with grpc.aio.insecure_channel("localhost:"+contact_server_list[b]) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    try:
                        response = await stub.ClientTransaction(request) 
                        print("Done with second phase")
                        print(response)
                    except Exception as e:
                        print(e)
            print(f"TwoPhase = {TwoPhase}")
            if response.response == "ABORTED":
                print("Failed in the second phase proceeding ROLLBACK")
                cleaned[2] = str(int(cleaned[2])*-1)
                s = f"({', '.join(cleaned)})"
                request = project4_pb2.ClientTransactionRequest(
                        name=cleaned[0],
                        message=s,
                        timestamp=timestamp,
                        signature=signature_1,
                        server_list = server_list_1,
                        byzantine_list = list(byzantine_list),
                        isIntraShard = False
                        #view_number = view_number
                    )
                async with grpc.aio.insecure_channel("localhost:"+contact_server_list[a]) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    try:
                        response = await stub.ClientTransaction(request) 
                        print("Done with ROLLBACK")
                        print(response)
                    except Exception as e:
                        print(e)
            return project4_pb2.ClientTransactionCrossShardResponse(response=True)
        except Exception as e:
            print("Exception in Cross Shard Transaction")
            print(e)


    async def ClientTransaction(self, request, context):
        print("Started")
        if self.pending_transactions_cross_shard and not request.isPendingTrans:
            return project4_pb2.ClientTransactionResponse(response="ABORTED")
        if self.pending_transactions and not request.isPendingTrans:
            while self.pending_transactions:
                self.sequence_number, pending_request = self.pending_transactions.popleft()
                print("pending request")
                #pending_request.server_list = request.server_list
                new_request = project4_pb2.ClientTransactionRequest(
                    name=pending_request.name,
                    message=pending_request.message,
                    timestamp=pending_request.timestamp,
                    signature=pending_request.signature,
                    server_list = request.server_list,
                    byzantine_list = request.byzantine_list,
                    isIntraShard = pending_request.isIntraShard,
                    isRollBack = pending_request.isRollBack,
                    contact_server_list = request.contact_server_list,
                    isPendingTrans = True
                )
                async with grpc.aio.insecure_channel("localhost:"+self.port) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    response = await stub.ClientTransaction(new_request)
                    print("Sent pending transaction")
                    print(new_request.message)

                    print(response)
                
        print(">> L 1 CLIENT TRANSACTION")
        print(request.message)
        print('##############')
        cleaned = request.message.strip("()").replace("'", "").split(",")
        cleaned = [x.strip(" ") for x in cleaned]
        isIntrashard = request.isIntraShard
        while cleaned[0] in self.locks or cleaned[1] in self.locks:
            print("Encountering Locks")
            await asyncio.sleep(0.1)
        if isIntrashard:
            ClientQuery = Query()
            client_a_balance = self.db.get(ClientQuery.client_id == int(cleaned[0]))
            client_a_balance = client_a_balance['balance'] if client_a_balance else None
            if client_a_balance < int(cleaned[2]):
                return project4_pb2.ClientTransactionResponse(response="ABORTED")
        else:
            print(f"<> one {int(cleaned[0])//1000} two {(int(self.port[4:])-51)//4}")
            if int(cleaned[0])//1000 == (int(self.port[4:])-51)//4:
                print("Im in")
                ClientQuery = Query()
                client_a_balance = self.db.get(ClientQuery.client_id == int(cleaned[0]))
                client_a_balance = client_a_balance['balance'] if client_a_balance else None
                print(f"Client A Balance = {client_a_balance}")
                print(f"Amt = {cleaned[2]}")
                if int(client_a_balance) < int(cleaned[2]):
                    return project4_pb2.ClientTransactionResponse(response="ABORTED")
            else:
                ClientQuery = Query()
                client_b_balance = self.db.get(ClientQuery.client_id == int(cleaned[1]))
                client_b_balance = client_b_balance['balance'] if client_b_balance else None
    
        
        self.log += str((request.message,
                     self.sequence_number,self.view_number))
        
        try:
            self.locks[cleaned[0]] = 1
            self.locks[cleaned[1]] = 1
            print("Locks acquired")
            print(self.locks)
            print(f"Received transaction from client {request.message}")
            print(request)
            try:
                public_key = load_public_key_client(request.name)
                self.verify_signature(public_key,request,context)
            except Exception as e:
                print(f"Error in signature verification: {str(e)}")
            #if self.pending_transactions
                #if  min(self.pending_transactions, key=lambda x: x[0]) < self.sequence_number+1:
                    #print("Kuch toh gadbad hai")
            if not request.isPendingTrans:
                self.sequence_number = self.increment_sequence_number()
            #if not request.isRollBack:
            transaction_sequence_number = self.sequence_number
            self.flag1[self.sequence_number] = 0
            self.flag2[self.sequence_number] = 0
            digest = self.collision_resistant_hash(request.message)
            l = []
            l.append(list(ast.literal_eval(request.message)))
            l.append(request.timestamp)
            l.append("NA")
            l.append(digest)
            self.transaction_list[self.sequence_number] = l
            response = await self.sendPrePrepare(request.server_list,self.view_number,request,request.byzantine_list,digest,isIntrashard,transaction_sequence_number,request.isPendingTrans,request.cross_shard_request)
            print(f"L1 {response}")
            return project4_pb2.ClientTransactionResponse(response="ABORTED"if response==False else "ACCEPTED")
    
            
        except Exception as e:
            print(f"Error in transaction: {str(e)}")
        return project4_pb2.ClientTransactionResponse(response=True) 
    async def sendPrePrepare(self,server_list,view_number,request,byzantine_list,digest,isIntrashard,transaction_sequence_number,isPendingTrans,r):
        
        tasks = [
                self.PrePrepare(server,digest,self.sequence_number,self.view_number, request,server_list,byzantine_list,isIntrashard) 
             for server in server_list if server!= str(self.port)
            ]
        result = await asyncio.gather(*[
        asyncio.create_task(task) for task in tasks
            ])
        result = [x.response for x in result]
        #result = [x for x in A if x not in B]
        byz = [p for p in request.byzantine_list if p in request.server_list]
        print("RESULTT")
        print(result)
        print(byz)
        if len(result)-len(byz)<2 and not isPendingTrans:
            await asyncio.sleep(1)
            if isIntrashard:
                self.pending_transactions.append((transaction_sequence_number,request))
            else:
                self.pending_transactions_cross_shard.append((transaction_sequence_number,r)) 
            for seq in self.transaction_list:
                x = self.transaction_list[seq][0][0]
                y = self.transaction_list[seq][0][1]
                if str(x) in self.locks:
                    del self.locks[str(x)]
                if str(y) in self.locks:
                    del self.locks[str(y)]
                    print("Freed Locks")
            print("QUEUE")
            print("RESULTT")
            print(result)
            print("Intrashard queue")
            print(self.pending_transactions)
            print("Cross shard queue")
            print(self.pending_transactions_cross_shard)
            print
            return False
        
        elif len(result)>=2 and "ACCEPTED" not in result:
            print("RESULTT")
            print(result)
            if isIntrashard:
                self.pending_transactions.append((transaction_sequence_number,request))
            else:
                self.pending_transactions_cross_shard.append((transaction_sequence_number,r))
            print("Intrashard QUEUE")

            print(self.pending_transactions)
            print("Cross shard QUEUE")
            print(self.pending_transactions_cross_shard)
            return False
        #print(f"L2 {result}")
        #return "ACCEPTED" in result
        print("SAB THIK")
        return True
    
    async def PrePrepare(self,server,digest,sequence_number,view_number,client_request,server_list,byzantine_list,isIntrashard):
        print(">> L 2 SEND PREPREPARE")
        print(f"{self.port} sending PrePrepare to {server}")
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
                
                stub = project4_pb2_grpc.project4ServiceStub(channel)
                print(f"In PrePrepare Phase, sending PrePrepare to server{server}")
                try:
                    signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(self.port)}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(self.port)}: {sign_error}")
                    return None
                request = project4_pb2.PrePrepareRequest(
                    digest = digest,
                    sequence_number = sequence_number,
                    view_number = view_number,
                    signature=signature,
                    client_request = client_request,
                    name = str(self.port),
                    server_list = server_list,
                    byzantine_list = byzantine_list,
                    isIntraShard = isIntrashard
                )
                try:
                    print("FP1")
                    #print(request)
                    response = await stub.ReplicaPrePrepare(request)
                    print(f"L3 {response}")                 
                    return project4_pb2.PrePrepareResponse(response=response.response)
                
                except grpc.RpcError as e:
                    print(f"RPC failed for preprep {server} with {e.code()}: {e.details()}")
                    return None
    async def ReplicaPrePrepare(self,request,context):
        print(">> R 1 RECEIVE PREPREPARE")
        try:
            print("PREPARE RECEIVED")
            cleaned = request.client_request.message.strip("()").replace("'", "").split(",")
            cleaned = [x.strip(" ") for x in cleaned]
            print(cleaned)
            #if cleaned[0] in self.locks or cleaned[1] in self.locks:
                #self.rep_pending_queue.append(request.message)
           # cond = cleaned[0] in self.locks or cleaned[1] in self.locks
            while cleaned[0] in self.locks or cleaned[1] in self.locks:
                #print("Encoutering Locks")
                await asyncio.sleep(0.1)
            self.locks[cleaned[0]] = 1
            self.locks[cleaned[1]] = 1
            print("Locks acquired")
            print(self.locks)
            self.log += str((request.client_request.message,
                    self.sequence_number,self.view_number))
            #await self.start_timer(request.server_list,request.byzantine_list)
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
                        result = await self.sendPrepare(digest,view_number,signature,sequence_number,request.name,request.server_list, request.byzantine_list,request.isIntraShard)
                        print(f"L4 {result}")
                       # await self.stop_timer()
                        return project4_pb2.PrePrepareResponse(response=result)
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
    async def sendPrepare(self,digest,view_number,signature,sequence_number,name,server_list,byzantine_list, isIntrashard):
        print(">> R 2 SEND PREPARE")
        async with grpc.aio.insecure_channel("localhost:"+name) as channel:
            stub = project4_pb2_grpc.project4ServiceStub(channel)
            print(f"In Prepare Phase, sending Prepare to server{name}")
            request = project4_pb2.PrepareRequest(
                        digest = digest,
                        sequence_number = sequence_number,
                        view_number = view_number,
                        signature=signature,
                        name = str(self.port),
                        server_list = server_list,
                        byzantine_list = byzantine_list,
                        isIntrashard = isIntrashard
                    )
            try:
                response = await stub.Prepare(request)
                print(f"L5 {response}")   
                return response.response                 
                #return response.response
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def Prepare(self,request,context):
        print(">> L 3 PREPARE")
        view = request.view_number
        if self.port in request.byzantine_list:
            print("Byzantine leader alert")
            while(self.view_number == view):
                #print("AAAAAAAA")
                await asyncio.sleep(0.1)
            print(f"L6 False")
            return project4_pb2.PrepareResponse(response=False)
        print(f"I the primary server {self.port} received prepare request from {request.name} with sequence number {request.sequence_number}")
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        if request.sequence_number not in self.vote_count:
            self.vote_count[request.sequence_number] = 2
        else:
            self.vote_count[request.sequence_number] += 1
        if request.digest != self.transaction_list[
            request.sequence_number][3]:
            return project4_pb2.PrepareResponse(response=False)
        if request.sequence_number not in self.request_block:
            a = []
            a.append(request)
            self.request_block[request.sequence_number] = a
        else:
            self.request_block[request.sequence_number].append(request)
        await asyncio.sleep(0.1)
        print("Phase 3 successful")
        if (self.vote_count[request.sequence_number]>2) and self.flag1[request.sequence_number]==0:
    
            self.flag1[request.sequence_number]=1
            server_list = request.server_list
            digest = request.digest
            self.transaction_list[request.sequence_number][2] = "PREPARED"
            tasks = [
                self.sendReady(server,self.request_block[request.sequence_number],request.sequence_number,digest,server_list,request.byzantine_list,request.isIntrashard) 
             for server in server_list if server!= str(self.port)
            ]
            result = await asyncio.gather(*[
            asyncio.create_task(task) for task in tasks
                ])
            print(f"L7 uraaaa {result}")
            return project4_pb2.PrepareResponse(response='ACCEPTED')
        
        return project4_pb2.PrepareResponse(response="ABORTED")
    async def sendReady(self,server,request_block,sequence_number,digest,server_list,byzantine_list,isIntrashard):
        print(">> L 4 SEND READY")
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
            stub = project4_pb2_grpc.project4ServiceStub(channel)
            print(f"In Ready Phase, sending Ready to serverzz {server} with sequence number {sequence_number}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))                   
            request = project4_pb2.PrepareBroadcastRequest(
                            view_number = self.view_number,
                            sequence_number = sequence_number,
                            prepare_requests_block = request_block,
                            name = str(self.port),
                            signature=signature,
                            digest = digest,
                            server_list = server_list,
                            byzantine_list = byzantine_list,
                            isIntrashard = isIntrashard
            )

            try:
                response = await stub.PrepareBroadcast(request)  
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def PrepareBroadcast(self, request, context):
        print(">> R 3 RECEIVE READY")
        print(f"Received Prepare Broadcast from {request.name}")
        print(f"The length of request block is {len(request.prepare_requests_block)}")     
        print(request.prepare_requests_block)     
        digest = request.digest
        print(f"Prepare Broadcast recieved by {self.port}")
        print(len(request.prepare_requests_block))
        request_block = request.prepare_requests_block
        public_key_server = load_public_key_server(request.name)
        self.verify_signature(public_key_server,request,context)
        print("<> before")
        print(self.transaction_list)
        print("sequence number")
        print(request.sequence_number)
        if 0==0:
            print(self.port not in request.byzantine_list)
            if self.port not in request.byzantine_list:
                self.transaction_list[request.sequence_number][2] = "PREPARED"
                print("<> AFTER")
                print(self.transaction_list)
        
                await self.sendCommit(request.name,request.sequence_number,request.digest,request.server_list,request.byzantine_list,request.isIntrashard)
                return project4_pb2.PrepareBroadcastResponse(response=True)
        return project4_pb2.PrepareBroadcastResponse(response=True)
    async def sendCommit(self,name,sequence_number,digest,server_list,byzantine_list,isIntrashard):
        print(">> R 4 SEND COMMIT")
        async with grpc.aio.insecure_channel("localhost:"+name) as channel:
            stub = project4_pb2_grpc.project4ServiceStub(channel)
            print(f"In Commit Phase, sending Commit to server{name}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
            request = project4_pb2.CommitRequest(
                        sequence_number = sequence_number,
                        digest = digest,
                        name = str(self.port),
                        signature=signature,
                        server_list = server_list,
                        byzantine_list = byzantine_list,
                        isIntrashard = isIntrashard
                    )
            try:
                response = await stub.Commit(request)                    
                return response
            except grpc.RpcError as e:
                print(f"RPC failed with {e.code()}: {e.details()}")
                return None
    async def Commit(self,request,context):
        print(">> L 5 COMMIT")
        print(f"I am {self.port} and I received Commit from {request.name} with sequence number {request.sequence_number}")
        print(request)
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
        if self.vote_count_commit[request.sequence_number]>2 and self.flag2[request.sequence_number]==0:
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
                            print(f"Check 9 x = {x} y = {y} amt = {amt}")
                            ClientQuery = Query()
                            client_a_balance = self.db.get(ClientQuery.client_id == int(x))
                            client_a_balance = client_a_balance['balance'] if client_a_balance else None
                            client_b_balance = self.db.get(ClientQuery.client_id == int(y))
                            client_b_balance = client_b_balance['balance'] if client_b_balance else None
                            print(f"client_a_balance = {client_a_balance} client_b_balance = {client_b_balance}")
                            print(f"IsIntrashard = {request.isIntrashard}")
                            #isIntrashard = True
                            if request.isIntrashard:
                                print("Inside aaya")
                                print(self.locks)
                                print(type(x))
                                self.db.update({'balance': client_a_balance - int(amt)}, ClientQuery.client_id == int(x))
                                self.db.update({'balance': client_b_balance + int(amt)}, ClientQuery.client_id == int(y))
                                #filtered_deque = deque(item for item in my_deque if item[0] != value_to_remove)
                                self.pending_transactions = deque(item for item in self.pending_transactions if item[0] != seq)
                                print("Intra Shard QUEUE")
                                #print(self.pending_transactions)
                                print(self.pending_transactions)
                                print("Cross Shard QUEUE")
                                print(self.pending_transactions_cross_shard)
                                if str(x) in self.locks:
                                    del self.locks[str(x)]
                                if str(y) in self.locks:
                                    del self.locks[str(y)]
                               
                                print("Locks released")
                                print(self.locks)
                            else:
                                if client_a_balance is not None:
                                    self.db.update({'balance': client_a_balance - int(amt)}, 
                                                ClientQuery.client_id == int(x))
                                else:
                                    print("<< Check 7")
                                    self.db.update({'balance': client_b_balance + int(amt)}, ClientQuery.client_id == int(y))
                                    #self.client_balances[int(x[1])]+=int(x[2])
                                    print("Client Balances ")
                            #if self.balances[x] - amt >= 0:
                                #print(f"Executing {x}-{amt} and {y}+{amt}")
                                #self.balances[x] = self.balances[x] - amt
                                #self.balances[y] = self.balances[y] + amt
                        else:
                            flag3 = 1
                server_list = request.server_list
                for seq in self.transaction_list:
                    x = self.transaction_list[seq][0][0]
                    y = self.transaction_list[seq][0][1]
                    if str(x) in self.locks:
                        del self.locks[str(x)]
                    if str(y) in self.locks:
                        del self.locks[str(y)]
                    print("Locks released")
                    print(self.locks)
                tasks = [
                    self.sendCommitBroadcast(server,self.request_block_commit[request.sequence_number],request.sequence_number,request.digest,server_list,request.byzantine_list,request.isIntrashard) 
                for server in server_list if server!= str(self.port)
                ]
                result = await asyncio.gather(*[
                asyncio.create_task(task) for task in tasks
                    ])
                print("Final Balances")
                #print(self.balances)
                if flag3 == 1:
                    return project4_pb2.CommitResponse(response=False)
        
        return project4_pb2.CommitResponse(response=True)
    async def sendCommitBroadcast(self,server,request_block_commit,sequence_number,digest,server_list,byzantine_list,isIntrashard):
        print(">> R 5 SEND COMMIT BROADCAST")
        async with grpc.aio.insecure_channel("localhost:"+server) as channel:
            stub = project4_pb2_grpc.project4ServiceStub(channel)
            print(f"In Commit Broadcast Phase, sending Commit Broadcast to server{server} with sequence number {sequence_number}")
            signature = sign_message(f"./keys/server_keys/private_key_{str(self.port)}.pem",str(self.port))
            request = project4_pb2.CommitBroadcastRequest(
                        sequence_number = sequence_number,
                        digest = digest,
                        name = str(self.port),
                        signature=signature,
                        commit_requests_block = request_block_commit,
                        server_list = server_list,
                        byzantine_list = byzantine_list,
                        isIntrashard = isIntrashard
                    )
            try:
                response = await stub.CommitBroadcast(request)                    
                return response
            except grpc.RpcError as e:
                print(f"RPC failed in Commit Broadcast with {e.code()}: {e.details()}")
                return None
    async def CommitBroadcast(self,request,context):
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
                            print(f"CHEck 10 x={x} y={y} amt={amt}")
                            ClientQuery = Query()
                            client_a_balance = self.db.get(ClientQuery.client_id == int(x))
                            client_a_balance = client_a_balance['balance'] if client_a_balance else None
                            client_b_balance = self.db.get(ClientQuery.client_id == int(y))
                            client_b_balance = client_b_balance['balance'] if client_b_balance else None
                            #isIntrashard = True
                            if request.isIntrashard:
                                print("Inside aaya")
                                print(self.locks)
                                self.db.update({'balance': client_a_balance - int(amt)}, ClientQuery.client_id == int(x))
                                self.db.update({'balance': client_b_balance + int(amt)}, ClientQuery.client_id == int(y))
                                try:
                                    if str(x) in self.locks:
                                        del self.locks[str(x)]
                                    if str(y) in self.locks:
                                        del self.locks[str(y)]
                                    print("Locks released")
                                    print(self.locks)
                                except Exception as e:
                                    print("E1")
                            else:
                                #if int(x[0]) in self.client_balances:
                                if client_a_balance is not None:
                                    self.db.update({'balance': client_a_balance - int(amt)}, 
                                                ClientQuery.client_id == int(x))
                                    print(f"Redacting {amt} from {x}")
                                else:
                                    print("<< Check 7")
                                    self.db.update({'balance': client_b_balance + int(amt)}, ClientQuery.client_id == int(y))
                                    #self.client_balances[int(x[1])]+=int(x[2])
                                    print("Client Balances ")
                        flag4 = 1
                print(f"Final Balances->{request.sequence_number}")
                #print(self.balances)
                if flag4 == 0:
                    try:
                        for seq in self.transaction_list:
                            x = self.transaction_list[seq][0][0]
                            y = self.transaction_list[seq][0][1]
                            if str(x) in self.locks:
                                del self.locks[str(x)]
                            if str(y) in self.locks:
                                del self.locks[str(y)]
                                print("Locks released")
                                print(self.locks)
                        print("Returning 1")
                        return project4_pb2.CommitBroadcastResponse(response=True)
                    except Exception as e:
                        print("E2")
        try:
            for seq in self.transaction_list:
                x = self.transaction_list[seq][0][0]
                y = self.transaction_list[seq][0][1]
                if str(x) in self.locks:
                    del self.locks[str(x)]
                if str(y) in self.locks:
                    del self.locks[str(y)]
                    print("Locks released")
                    print(self.locks)
            print("Returning 2")
            return project4_pb2.CommitBroadcastResponse(response=False)
        except Exception as e:
            print("E3")
async def serve(port, server_name):
    server = grpc.aio.server() 
    project4_pb2_grpc.add_project4ServiceServicer_to_server(Server(server_name,port), server)
    server.add_insecure_port(f'localhost:{port}')
    await server.start()
    print(f"Server {server_name} running on port {port}")
    await server.wait_for_termination() 

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python server.py <port> <server_name>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    server_name = sys.argv[2]
    asyncio.run(serve(port, server_name))