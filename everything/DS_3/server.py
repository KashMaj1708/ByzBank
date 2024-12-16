import asyncio
import grpc
import transactions_pb2
import transactions_pb2_grpc
import sys
import random
from collections import deque
from tinydb import TinyDB, Query

class Server(transactions_pb2_grpc.TransactionServiceServicer):
    def __init__(self, name, port):
        self.transactions = []
        self.balance = 100
        self.name = name
        self.tcount = 1
        self.idp = 1
        self.port = str(port)
        self.promise_idp = -1
        self.promise_count = 1
        self.flag = 0
        self.transaction_block = []
        self.accept_count = 1
        self.flag_1 = 0
        self.flag_2 = 0
        self.prepare_idp = 1
        self.accept_idp = -1
        self.flag_3 = 0
        self.persistent_storage = []
        self.flag_4 = 0
        self.pending_transactions = []
        self.locks = {}
        self.persisted_transactions = []
        #self.client_balances = {}
        self.db = TinyDB(f'client_balances_{self.port}.json')
        self.initialize_client_balances()
        #self.print_all_client_balances()
        # if self.port in ['500051','500052','500053']:
        #     self.client_balances = {100: 10, 501: 10, 299: 10, 101: 10, 301: 10, 1: 10, 796: 10, 997: 10, 196: 10, 197: 10, 973: 10, 707: 10, 333: 10, 691: 10, 11: 10, 121: 10, 601: 10}

        # elif self.port in ['500054','500055','500056']:
        #     self.client_balances = {1001: 10, 1650: 10, 1201: 10, 1111: 10, 1895: 10, 1890: 10, 1990: 10, 1495: 10, 1490: 10, 1690: 10, 1695: 10, 1999: 10, 1500: 10, 1505: 10, 1997: 10, 1998: 10}
        # else:
        #     self.client_balances = {2800: 10, 2150: 10, 2995: 10, 2990: 10, 2994: 10, 2770: 10, 2799: 10, 2975: 10, 2970: 10, 2999: 10, 2001: 10, 2525: 10, 2596: 10, 2297: 10, 2196: 10, 2397: 10, 2998: 10}
    def initialize_client_balances(self):
        if self.port in ['500051', '500052', '500053']:
            client_range = range(1, 1000)
        elif self.port in ['500054', '500055', '500056']:
            client_range = range(1001, 2000)
        elif self.port in ['500057', '500058', '500059']:
            client_range = range(2001, 3000)
        else:
            client_range = []

        # Initialize balances in the database
        for client_id in client_range:
            self.db.insert({'client_id': client_id, 'balance': 10})

        # Load balances into the client_balances dictionary for use in the server
        #self.client_balances = {item['client_id']: item['balance'] for item in self.db.all()}
    def print_all_client_balances(self):
        print("Current Client Balances:")
        for record in self.db.all():
            print(f"Client ID: {record['client_id']}, Balance: {record['balance']}")
    
    def GetBalance(self,request,context):
        ClientQuery = Query()
        client_balance = self.db.get(ClientQuery.client_id == request.client_id)
        return transactions_pb2.GetBalanceResponse(balance= str(client_balance['balance']) if client_balance else "NA")
    
    def GetDB(self,request,context):
        return transactions_pb2.GetDBResponse(DB = self.persistent_storage)

    def GetLogs(self,request,context):
        print("## Sending logs")
        print(self.persisted_transactions)
        return transactions_pb2.GetLogResponse(logs = self.persisted_transactions)
    
    def UpdateDB(self,request,context):
        max_DB = request.max_DB
        print(max_DB)
        if len(max_DB)>len(self.persistent_storage):
            difference = list(set(max_DB)-set(self.persistent_storage))
            for i in difference:
                x = i.split(':')
                if x[1] == self.port:
                    self.balance += int(x[2])
            self.persistent_storage = max_DB
            return transactions_pb2.UpdateDBResponse(success = True)
        return transactions_pb2.UpdateDBResponse(success = False)
    
    def GetPendingTransactions(self,request,context):
        if(len(self.pending_transactions)>0):
            sentdex = [":".join(map(str, x)) for x in self.pending_transactions]
            print("SENTDEXXXXXXX")
            print(self.pending_transactions)
            print(sentdex)
            self.pending_transactions = []

            return transactions_pb2.GetPendingTransactionsResponse(pending_transactions = sentdex)
        return transactions_pb2.GetPendingTransactionsResponse(pending_transactions = [])
    def SetPendingTransactions(self,request,context):
        self.pending_transactions.append(
            (request.from_user,request.to_user,request.amount)
        )
        print("pending transactions for ",self.port)
        print(self.pending_transactions)
       # self.pending_transactions.append(A+":"+B+":"+str(amount)+":"+str(self.tcount))
        self.flag_4 = 0
        self.promise_idp = -1
        self.promise_count = 1
        self.flag = 0
        self.transaction_block = []
        self.accept_count = 1
        self.flag_1 = 0
        self.flag_2 = 0
        self.prepare_idp = 1
        self.accept_idp = -1
        self.flag_3 = 0  
        self.balance_dict = {"100":10,"501":8}
        self.locks = {}
        return transactions_pb2.SetPendingTransactionsResponse(message=True)
    async def Catchup(self,request,context):
        try:
            print("CATCH UP")
            recent_logs = request.logs
            n = len(self.persisted_transactions)
            diff = len(recent_logs) - n
            l1 = []
            for i in range(0,diff):
                l1.append(recent_logs[n+i])
                s1 = recent_logs[n+i].split(':')
                print(s1)
                if len(s1) == 4:
                    ClientQuery = Query()
                    client_a_balance = self.db.get(ClientQuery.client_id == int(s1[1]))
                    client_a_balance = client_a_balance['balance'] if client_a_balance else None
                    client_b_balance = self.db.get(ClientQuery.client_id == int(s1[2]))
                    client_b_balance = client_b_balance['balance'] if client_b_balance else None
                    if client_a_balance is not None:
                        self.db.update({'balance': client_a_balance - int(s1[3])}, ClientQuery.client_id == int(s1[1]))
                    if client_b_balance is not None:
                        self.db.update({'balance': client_b_balance + int(s1[3])}, ClientQuery.client_id == int(s1[2]))
                elif len(s1) == 5:
                    if s1[4]=='C':
                        ClientQuery = Query()
                    client_a_balance = self.db.get(ClientQuery.client_id == int(s1[1]))
                    client_a_balance = client_a_balance['balance'] if client_a_balance else None
                    client_b_balance = self.db.get(ClientQuery.client_id == int(s1[2]))
                    client_b_balance = client_b_balance['balance'] if client_b_balance else None
                    if client_a_balance is not None:
                        self.db.update({'balance': client_a_balance - int(s1[3])}, ClientQuery.client_id == int(s1[1]))
                    if client_b_balance is not None:
                        self.db.update({'balance': client_b_balance + int(s1[3])}, ClientQuery.client_id == int(s1[2]))
            print("CATCH UP successful for ",self.port)
            self.persisted_transactions.extend(l1)
            return transactions_pb2.CatchupResponse(message=True)
        except Exception as e:
            print("CATCH UP failed for ",self.port)
            print(e)
            return transactions_pb2.CatchupResponse(message=False   )
    async def ModifyDataStore(self,request,context):
        print(f"Received transaction {request.transaction} from {self.port}")
        t = f'<{self.prepare_idp - 1},{self.port[5]}>:{request.transaction}:C'
        self.persisted_transactions.append(t)
        try: 
            tasks = [
                    self.PrepareDatastore(server_address,t)
                    for server_address in request.server_list
                    if server_address != self.port
                ]
            await asyncio.gather(*tasks)
        except Exception as e:
                        print(f"Error in ModifyDataStore: {e}")
        return transactions_pb2.ModifyDataStoreResponse(message=True)
    async def PrepareDatastore(self,server_address,t):
        print(f"Sending transaction {t} to {server_address}")
        async with grpc.aio.insecure_channel('localhost:'+server_address) as channel:
            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
            request = transactions_pb2.SendDataStoreRequest(data = t)
            try:
                response = await stub.SendDataStore(request)
            except Exception as e:
                print(f"Error in PrepareDatastore: {e}")
        return response.message
    
    async def SendDataStore(self,request,context):
        print(f"Received transaction {request.data} from {self.port}")
        print(type(request.data))
        self.persisted_transactions.append(request.data)
        return transactions_pb2.SendDataStoreResponse(message=True)
    async def Transact(self, request, context):
        transaction_queue = deque()
        # if self.port in ['500051','500052','500053']:
        #     self.client_balances = {100:10,200:10,300:10}
        # elif self.port in ['500054','500055','500056']:
        #     self.client_balances = {1650:10}
        A = request.from_user
        B = request.to_user
        print(f'From {A} to {B}, with amount {request.amount}')
        amount = request.amount
        server_list = request.serverList
        transaction = A+':'+B+':'+str(amount)
        print(f'''Executing transaction number {self.tcount} 
              on {self.port}''')
        transaction_queue.append(transaction)
        checker = 0
        #await asyncio.sleep(0.1)
        while len(transaction_queue)>0:
            print(f"Transaction Queue is {transaction_queue}")
            print(f"checker is {checker}")
            checker+=1
            transaction = transaction_queue.popleft()
            print(f"Executing paxos for transaction {transaction}")
            try:
                response = await self.send_prepare(server_list,transaction,request.isIntraShard)
            except Exception as e:
                print(f"Error in transaction: {e}")
            print(f"Response for transaction {transaction} is {response}")
            if response:
                print("Transaction Successful")
                #self.transactions.append(transaction)
                self.tcount+=1
                if request.isIntraShard:
                    t = f'<{self.prepare_idp - 1},{self.port[5]}>:{transaction}'
                    self.persisted_transactions.append(t)
                    try: 
                        tasks = [
                    self.PrepareDatastore(server_address,t)
                    for server_address in server_list
                    if server_address != self.port
                ]
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        print(f"Error in PrepareDataStore: {e}")
                else:
                    #t = transaction +':P'
                    #self.persisted_transactions.append(t)
                    if not request.isRollBack:
                        t = f'<{self.prepare_idp - 1},{self.port[5]}>:{transaction}:P'
                    else:
                        t1 = transaction.split(':')
                        t1[0],t1[1] = t1[1],t1[0]
                        transaction = ':'.join(t1)
                        t = f'<{self.prepare_idp - 1},{self.port[5]}>:{transaction}:A'
                    self.persisted_transactions.append(t)
                    try: 
                        tasks = [
                    self.PrepareDatastore(server_address,t)
                    for server_address in server_list
                    if server_address != self.port
                ]
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        print(f"Error in PrepareDataStore: {e}")

                return transactions_pb2.TransactionResponse(message='ACCEPTED')
            else:
                print("Transaction Failed")
                if not request.isIntraShard:
                    #t = transaction +':A'
                    #self.pending_transactions.append(t)
                    t = f'<{self.prepare_idp - 1},{self.port[5]}>:{transaction}:A'
                    self.persisted_transactions.append(t)
                    try: 
                        tasks = [
                    self.PrepareDatastore(server_address,t)
                    for server_address in server_list
                    if server_address != self.port
                ]
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        print(f"Error in PrepareDataStore: {e}")
                return transactions_pb2.TransactionResponse(message='ABORTED')
                #transaction_queue.append(transaction)
        #print(f"Executing paxos for transaction {transaction}")
        #response = await self.send_prepare(server_list,transaction)
        return transactions_pb2.TransactionResponse(message=response)                
            
                


    async def send_prepare(self,server_list,transaction,isIntraShard):
        #await asyncio.sleep(random.uniform(0, 0.1)) 
        tasks = [
            self.Prepare(server_address, self.prepare_idp, server_list,transaction,isIntraShard)
            for server_address in server_list
            if server_address != self.port
        ]
        response = await asyncio.gather(*tasks)
        #await asyncio.sleep(2)
        self.prepare_idp +=1
        print(f"Prepare response is {response}")
    
        return 'ABORTED' not in response and len(response) != 0

    async def Prepare(self,server_address,prepare_idp, server_list,transaction,isIntraShard):
        print(f"Server {self.port} sending prepare request to {server_address} with prepare_id {prepare_idp}")
        self.flag_1 = 0
        async with grpc.aio.insecure_channel('localhost:'+server_address) as channel:
                
                stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                request = transactions_pb2.PromiseRequest(
                    proposer = self.port,prepare_idp =
                    prepare_idp, server_list = server_list,
                    transaction = transaction,
                    isIntraShard = isIntraShard
                )
                try:
                    response = await stub.Promise(request)
                except Exception as e:
                    print(f"Error in prepare: {e}")
        print(f'''Promise response is {response} for transaction {transaction} from
              server {server_address}''')
        return response.message

    
    async def Promise(self, request, context):
        #if self.port in ['500051','500052','500053']:
            #self.client_balances =  {100:10,200:10,300:10}
        #elif self.port in ['500054','500055','500056']:
            #self.client_balances = {1650:10}
        self.flag_1 = 0
        print(f'Got a proposal from {request.proposer} with prepare_id {request.prepare_idp} for transaction {request.transaction}')
        if(request.prepare_idp>=self.promise_idp):
            self.promise_idp = request.prepare_idp
            server_list  = request.server_list
            try:
                if self.accept_idp == -1:
                    #self.accept_idp = request.prepare_idp
                    async with grpc.aio.insecure_channel(f'localhost:{request.proposer}') as channel:
                        stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                        request = transactions_pb2.AcknowledgeRequest(promise_idp = self.promise_idp,
                                        local_transactions = self.transactions,
                                        acceptor = self.port,
                                        accept_idp = self.accept_idp,
                                        server_list = server_list,
                                        accept_val = [],
                                        transaction = request.transaction,
                                        isIntraShard = request.isIntraShard)
                        
                        response = await stub.Acknowledge(request)
                        print("Check 1")
                        x = response.message
                        print(f"Acknowledge response is {x}")
                        print(type(x))
                        #response = transactions_pb2.PromiseResponse(message=True)
                        if not x:
                            return transactions_pb2.PromiseResponse(message='ABORTED')
                        return transactions_pb2.PromiseResponse(message='ACCEPTED')
                else:
                    async with grpc.aio.insecure_channel(f'localhost:{request.proposer}') as channel:
                        stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                        request = transactions_pb2.AcknowledgeRequest (promise_idp = self.promise_idp,
                                        local_transactions = [],
                                        acceptor = self.port,
                                        server_list = server_list,
                                        accept_val = self.accept_val,
                                        accept_idp = self.accept_idp,
                                        transaction = request.transaction,
                                        isIntraShard = request.isIntraShard)   
                        
                        response = await stub.Acknowledge(request)
                        print("Check 1")
                        print(f"Acknowledge response is {response.message}")
                        return transactions_pb2.PromiseResponse(message=response.message)
            except Exception as e:
                print(f"Error in Promise: {e}")
        return transactions_pb2.PromiseResponse(message=False)
    
    async def Acknowledge(self,request,context):
        print(f"I am {self.port}, I got a vote from {request.acceptor} with transaction_block {request.local_transactions} and accept_idp {request.accept_idp} and transaction {request.transaction}")
        self.promise_count+=1
        server_list = request.server_list
        print(f'Transaction Block: {request.local_transactions}')
        self.transaction_block.extend(request.local_transactions)
        if request.accept_idp != -1:
            self.flag_3 = 1
            self.carrier = request.accept_val
        await asyncio.sleep(0.1)
        
        if self.promise_count>= (len(request.server_list)//2)+1 and self.flag!=1:
            self.flag = 1

            print(f"Declaring majority with {self.promise_count} votes")
            print(f"Transaction is {request.transaction}")
            self.transaction_block.extend(self.transactions)
            print(f"Transactions = {self.transaction_block}")
            print(server_list)
            if self.flag_3 == 1:
                self.transaction_block = self.carrier
            
            #INSERT LOCK AND BALANCE CHECKING LOGIC HERE
            num_clusters = 3
            cluster_number = 0
            l = request.transaction.split(':')
            a = int (l[0])
            b = int(l[1])
            if a in self.locks or b in self.locks:
                print(f"ABORTING DUE TO LOCK ON {str(a)} or {str(b)}")
                self.flag = 0
                self.transaction_block = []
                self.accept_count = 1
                self.flag_1 = 0
                self.flag_2 = 0
                self.prepare_idp = 1
                self.accept_idp = -1
                self.flag_3 = 0
                self.persistent_storage = []
                self.flag_4 = 0
                self.pending_transactions = []
                response = transactions_pb2.AcknowledgeResponse(message=False)
                print(response)
                return transactions_pb2.AcknowledgeResponse(message=False)
            ClientQuery = Query()
            client_a_record = self.db.get(ClientQuery.client_id == a)
            print(f"Client A Record: {client_a_record}")
            client_a_balance = client_a_record['balance'] if client_a_record else None
            print(f"Client A Balance: {client_a_balance} and transaction amount: {l[2]}")
            if client_a_balance is not None:
                print('!! 1')
                if 1==1:
                    print('!! 2')
    
                    if int(l[2])> client_a_balance:
                        print('!! 3')
                        self.flag = 0
                        self.transaction_block = []
                        self.accept_count = 1
                        self.flag_1 = 0
                        self.flag_2 = 0
                        self.prepare_idp = 1
                        self.accept_idp = -1
                        self.flag_3 = 0
                        self.persistent_storage = []
                        self.flag_4 = 0
                        self.pending_transactions = []
                        print(f'''
                            ABORTING DUE TO INSUFFICIENT BALANCE ON {str(a)} , 
                            the present balance is {client_a_balance} and the
                            requested amount is {l[2]}
                            ''')
                        # try:
                        #     response = transactions_pb2.AcknowledgeResponse(message=True)
                        # except Exception as e:
                        #     print(f"Error: {e}")
                        # print("Check 2")
                        # print(response)
                        return transactions_pb2.AcknowledgeResponse(message=False)
            print(f"TEST_1 {self.prepare_idp}")
            try:
                tasks = [
                    self.SendAccept(server_address, self.prepare_idp, 
                                    server_list,self.transaction_block,
                                    request.transaction,request.isIntraShard)
                    for server_address in server_list
                    if server_address != self.port
                ]

                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"Error in Acknowledge: {e}")
            #print(f"Debugging Done for acceptor {request.acceptor}")
            print("<< Check")
            return transactions_pb2.AcknowledgeResponse(message=True)
        if self.promise_count>= (len(request.server_list)//2)+1:
           
            return transactions_pb2.AcknowledgeResponse(message=True)
        else:
            print(f">> {self.promise_count} {self.flag_3}")
            return transactions_pb2.AcknowledgeResponse(message=False)

    async def SendAccept(self,server_address,accept_request_idp,
                         server_list,transaction_block,transaction,isIntraShard):
        print(f"TEST_2 Transaction is: {transaction}")
        print(f"Sending Acceptance Request to {server_address}")
        print(type(accept_request_idp))
        print(f"{accept_request_idp}  {transaction_block}  {server_list}  {self.port} ")
        print('localhost:'+server_address)
        #try:

        async with grpc.aio.insecure_channel('localhost:'+server_address) as channel:
                stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                request = transactions_pb2.AcceptRequest(
                accept_request_idp = accept_request_idp,
                transaction_block = transaction_block, 
                server_list = server_list,
                leader = str(self.port),
                transaction = transaction,
                isIntraShard = isIntraShard
                )
                print("TEST 3")
                print(request)
                try:
                    response = await stub.Accept(request)
                except Exception as e:
                    print(f"Error in SendAccept: {e}")
                print(f"TEST_6 {response}")
        #except Exception as e:
            #print('localhost:'+server_address)
            #print(f"Error in SendAccept: {e}")
            #print(f"Error in sending accept request to {server_address}")
                
        return True

    async def Accept(self,request,context):
        try:
            print("REQUEST")
            print(request)
            transaction_block = request.transaction_block
            server_list = request.server_list
            print(f'''I am {self.port}, I received an accept message 
                {request.transaction} with  {request.accept_request_idp}
                ''')
            if request.accept_request_idp>=self.promise_idp:
                self.accept_idp = request.accept_request_idp
                self.accept_val = request.transaction_block
                print(f"Now my accept idp is {self.accept_idp} and accept val is {self.accept_val}")
                transaction = request.transaction
                print(type(request.leader))
                #if self.port=='500053':
                    #await asyncio.sleep(3)
                try:
                    await asyncio.sleep(random.uniform(0, 1))
                    async with grpc.aio.insecure_channel(f'localhost:{request.leader}') as channel:
                        stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                        
                        request = transactions_pb2.AcceptedRequest(
                                            accept_idp = self.accept_idp,
                                            transaction_block = transaction_block,
                                            server_list = server_list,
                                            acceptor = self.port,
                                            transaction = transaction,
                                            isIntraShard = request.isIntraShard)
                        print("TEST 4 ")
                        print(request)
                        try:
                            response = await stub.Accepted(request)
                        except Exception as e:
                            print(f"Error in Accept: {e}")
                        print("TEST 5")
                        print(response)
                except Exception as e:
                    print(f"Error in sending accepted request to {request.leader}")
                    print(e)
            return transactions_pb2.AcceptResponse(message=True)
        except Exception as e:
            print(f"Error in Accept BLOCK: {e}")
    async def Accepted (self,request,context):
        print(f'''KINGG  I leader {self.port} have received accept messages from 
              {request.acceptor}''')
        self.accept_count+=1
        server_list = request.server_list
        #server_list = request.server_list
        await asyncio.sleep(random.uniform(0,0.2))
        print(self.flag_1)
        if self.accept_count>= (len(request.server_list)//2)+1 and self.flag_1!=1:
            self.flag_1 = 1
           
            print(f"Declaring majority with {self.accept_count} votes")
            # for i in request.transaction_block:
            #     x = i.split(':')
            #     if x[1] == self.port:
            #         self.balance += int(x[2])
            #self.persistent_storage.extend(request.transaction_block)
            x = request.transaction.split(':')
            isIntraShard = request.isIntraShard
            ClientQuery = Query()
            client_a_balance = self.db.get(ClientQuery.client_id == int(x[0]))
            client_a_balance = client_a_balance['balance'] if client_a_balance else None
            client_b_balance = self.db.get(ClientQuery.client_id == int(x[1]))
            client_b_balance = client_b_balance['balance'] if client_b_balance else None
            if isIntraShard:
                self.db.update({'balance': client_a_balance - int(x[2])}, ClientQuery.client_id == int(x[0]))
                self.db.update({'balance': client_b_balance + int(x[2])}, ClientQuery.client_id == int(x[1]))
                #ClientQuery.client_id == client_a_id)
                #ClientQuery = Query()
                #client_a_record = self.db.get(ClientQuery.client_id == client_a_id)
                #client_b_record = self.db.get(ClientQuery.client_id == client_b_id)
                #self.client_balances[int(x[0])]-=int(x[2])
                #self.client_balances[int(x[1])]+=int(x[2])
            else:
                print("<< Check 4")
                print(x)
                #print(self.client_balances)
                print(self.port)
                #print(int(x[0]) in self.clients)
                if client_a_balance is not None:
                    print("<< Check 5")
                    self.db.update({'balance': client_a_balance - int(x[2])}, ClientQuery.client_id == int(x[0]))
                    #self.client_balances[int(x[0])]-=int(x[2])
                    print("<< Check 6")
                else:
                    self.db.update({'balance': client_b_balance + int(x[2])}, ClientQuery.client_id == int(x[1]))
                    #self.client_balances[int(x[1])]+=int(x[2])
            print("New Balances")
            print(f"client_a_balance: {client_a_balance}, client_b_balance: {client_b_balance}")
            #print(self.client_balances)
            #self.transactions = list(set(self.transactions)- set(request.transaction_block))
            print(f"Amount {self.balance}")
            print(f"Transactions {self.persistent_storage}")
            tasks = [
                self.SendCommit(server_address, self.accept_idp, server_list,self.transaction_block,request.transaction,isIntraShard)
                for server_address in server_list
                if server_address != self.port
            ]
            await asyncio.gather(*tasks)
        return transactions_pb2.AcceptedResponse(message=True)
    
    async def SendCommit(self,server_address,ballot, server_list,transaction_block, transaction,isIntraShard):
        print(f'''I {self.port} am sending final commit to
              {server_address} with transaction {transaction}''')
        try:
            async with grpc.aio.insecure_channel('localhost:'+server_address) as channel:
                    stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                    request = transactions_pb2.CommitRequest(
                        ballot=ballot,transaction_block =
                        transaction_block, server_list = server_list,
                        leader = self.port,transaction = transaction,
                        isIntraShard = isIntraShard)
                    
                    response = await stub.Commit(request)
                
                    print(self.prepare_idp)
        except Exception as e:
            print(f"Error in sending commit request to {server_address}")
            print(e)
        self.promise_idp = -1
        self.promise_count = 1
        self.flag = 0
        self.transaction_block = []
        self.accept_count = 1
        #self.flag_1 = 0
        self.flag_2 = 0
        self.prepare_idp = 1
        self.accept_idp = -1
        self.flag_3 = 0
       # self.persistent_storage = []
        self.flag_4 = 0
        return True
        

    async def Commit(self,request,context):
        print(f'''I am {self.port} Received Final commit
              from {request.leader}''' )
        # for i in request.transaction_block:
        #     x = i.split(':')
        #     if x[1] == self.port:
        #         self.balance += int(x[2])
        # self.persistent_storage.extend(request.transaction_block)
        # self.transactions = list(set(self.transactions)- set(request.transaction_block))
        x = request.transaction.split(':')
        isIntrashard = request.isIntraShard
        ClientQuery = Query()
        client_a_balance = self.db.get(ClientQuery.client_id == int(x[0]))
        client_a_balance = client_a_balance['balance'] if client_a_balance else None
        client_b_balance = self.db.get(ClientQuery.client_id == int(x[1]))
        client_b_balance = client_b_balance['balance'] if client_b_balance else None
        if isIntrashard:
                #self.client_balances[int(x[0])]-=int(x[2])
                #self.client_balances[int(x[1])]+=int(x[2])
                self.db.update({'balance': client_a_balance - int(x[2])}, ClientQuery.client_id == int(x[0]))
                self.db.update({'balance': client_b_balance + int(x[2])}, ClientQuery.client_id == int(x[1]))
        else:
            #if int(x[0]) in self.client_balances:
            if client_a_balance is not None:
                self.db.update({'balance': client_a_balance - int(x[2])}, 
                               ClientQuery.client_id == int(x[0]))
            else:
                print("<< Check 7")
                self.db.update({'balance': client_b_balance + int(x[2])}, ClientQuery.client_id == int(x[1]))
                #self.client_balances[int(x[1])]+=int(x[2])
        print("Client Balances ")
        #print(self.client_balances)
        self.promise_idp = -1
        self.promise_count = 1
        self.flag = 0
        self.transaction_block = []
        self.accept_count = 1
        #self.flag_1 = 0
        self.flag_2 = 0
        self.prepare_idp = 1
        self.accept_idp = -1
        self.flag_3 = 0
       #self.persistent_storage = []
        self.flag_4 = 0
        print(f"Persistent Storage {self.persistent_storage}")
        print(f"Amount {self.balance}")
        return transactions_pb2.CommitResponse(message=True)


        



async def serve(port, server_name):
    server = grpc.aio.server()  # Use grpc.aio.server() for async support
    transactions_pb2_grpc.add_TransactionServiceServicer_to_server(Server(server_name,port), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    print(f"Server {server_name} running on port {port}")
    await server.wait_for_termination()  # Await this call for async server

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python server.py <port> <server_name>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    server_name = sys.argv[2]
    asyncio.run(serve(port, server_name))
