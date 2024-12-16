import asyncio
import grpc
import transactions_pb2
import transactions_pb2_grpc
import time 
import pandas as pd
import ast
TwoPhase = {}
server_list_global = ["500051","500052",
                   "500053","500054",
                   "500055",
                   "500056","500057",
                   "500058","500059"]
# async def execute_transaction(server,transaction,server_list_modified,contact_server,i,isIntraShard):
#     async with grpc.aio.insecure_channel(f'localhost:'+contact_server) as channel:
#                 stub = transactions_pb2_grpc.TransactionServiceStub(channel)
#                 request = transactions_pb2.TransactionRequest(
#                         from_user=str(i[0]),
#                         to_user= str(i[1]),
#                         amount=i[2],
#                         serverList = server_list_modified,
#                         isIntraShard = isIntraShard
#                     )
async def catchup_cluster(server_list):
    #print(f"Executing Catchup for {server_list}")
    max_len = -1
    latest_logs = []
    for i in server_list:
        async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
            request = transactions_pb2.GetLogRequest()
            try:
                response = await stub.GetLogs(request)
                log = response.logs
                if len(log)>max_len:
                    max_len = len(log)
                    latest_logs = log
                #print(f"DataStore for server {i}: {response.logs}")
                #print()
            except Exception as e:
                print(f"Failed to get datastore for server {i}")
                print(f"Error{e}")
    #print(max_len)
    #print(latest_logs)
    for i in server_list:
        async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
            request = transactions_pb2.GetLogRequest()
            try:
                response = await stub.GetLogs(request)
                log = response.logs
                if len(log)<max_len:
                    request = transactions_pb2.CatchupRequest(logs = latest_logs)
                    response = await stub.Catchup(request)
                #print(f"DataStore for server {i}: {response.logs}")
                #print()
            except Exception as e:
                print(f"Failed to get datastore for server {i}")
                print(f"Error{e}")
async def catchup(server_list,num_clusters):
    global TwoPhase
    TwoPhase = {}
    s = 9//num_clusters
    server_list_clustered = [[] for _ in range(num_clusters)]
    for i in range(len(server_list)):
        #print((int(server_list[i][5])-1)//num_clusters)
        server_list_clustered[(int(server_list[i][5])-1)//num_clusters].append(server_list[i])
    for j in server_list_clustered:
        await catchup_cluster(j)

async def cluster_transact(cluster,server_list,i,num_clusters,contact_server):
    #print(f"Cluster {i}")
    if i == 1:
        await asyncio.sleep(3)
    k = i
    server_list_modified = []
    s = (9//num_clusters)
    #if 9%num_clusters != 0 and i==0:
        #s+=1
   
    for j in range(len(server_list)):
        
        #print(i*num_clusters,int(server_list[j][5]),(i+1)*num_clusters)
        if 9%num_clusters != 0:
            if i==0:
                if i*s< int(server_list[j][5]) <= ((i+1)*s)+1:
                    server_list_modified.append(server_list[j])
            else:
                if (i*s)+1< int(server_list[j][5]) <= ((i+1)*s)+1:
                    server_list_modified.append(server_list[j])
        else:
            if i*s< int(server_list[j][5]) <= (i+1)*s:
                server_list_modified.append(server_list[j])
    #print(f"Server List modified {server_list_modified}")
    #contact_server = ["500051","500054","500057"]
    for i in cluster:
        if i[3] == -1:
            #print("Executing Intra Shard Transactions")
            async with grpc.aio.insecure_channel(f'localhost:'+contact_server[k]) as channel:
                stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                request = transactions_pb2.TransactionRequest(
                        from_user=str(i[0]),
                        to_user= str(i[1]),
                        amount=i[2],
                        serverList = server_list_modified,
                        isIntraShard = True,
                        isRollBack = False
                    )
                try:
                    #response = await stub.Transact(request,timeout = 10)
                    response = await stub.Transact(request)
                    #print(i)
                    #print(response)
                except Exception as e:
                    print("Transaction failed")
                    print(e)
        else:
            #print("Executing Cross Shard Transactions")
            #print(i)
            async with grpc.aio.insecure_channel(f'localhost:'+contact_server[k]) as channel:
                stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                request = transactions_pb2.TransactionRequest(
                        from_user=str(i[0]),
                        to_user= str(i[1]),
                        amount=i[2],
                        serverList = server_list_modified,
                        isIntraShard = False,
                        isRollBack = False
                    )
                try:
                    #response = await stub.Transact(request,timeout = 10)
                    response = await stub.Transact(request)
                except Exception as e:
                    print("Transaction failed 2")
                    print(e)
            #print("Executing Two Phase Commit")
            #print(time.time())
            #print(i)
            #print(f"Cluster {k}")
                    
                    #if response.message == 'ABORTED':
                        #print("Transaction Aborted")
                        #continue
                    #await asyncio.sleep(10)
            #print(TwoPhase)
            #print(f"Checking time {time.time()}")
            if i[3] not in TwoPhase:
                x = response.message
                TwoPhase[i[3]] = [x,i,k,server_list_modified]
                print("TWO PHASE DICTIONARY IS")
                print(TwoPhase)
            else:
                #print("inside else")
                #print(type(i))
                #print(i)
                i1 = [str(x) for x in i]
                s = (':').join(i1)
                #print(i1)
                #print(TwoPhase)
                #print(response.message)
                if TwoPhase[i[3]][0] == 'ACCEPTED':
                    if response.message == 'ACCEPTED':
                        #print("Cross Shard Transaction Accepted")
                        async with grpc.aio.insecure_channel(f'localhost:'+contact_server[k]) as channel:
                            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                            request = transactions_pb2.ModifyDataStoreRequest(
                                transaction = s,
                                server_list = server_list_modified
                    )
                            try:
                    #response = await stub.Transact(request,timeout = 10)
                                response = await stub.ModifyDataStore(request)
                            except Exception as e:
                                print("Transaction failed 2")
                                print(e)
                        p = TwoPhase[i[3]][1]
                        p1 = p[0:3]
                        p1 = [str(x) for x in p1]
                        p1 = (':').join(p1)
                        #print(f"P is {p}")
                        async with grpc.aio.insecure_channel(f'localhost:' + contact_server[TwoPhase[i[3]][2]]) as channel:
                            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                            request = transactions_pb2.ModifyDataStoreRequest(
                                transaction = p1,
                                server_list = TwoPhase[i[3]][3]
                                )
                            try:
                    #response = await stub.Transact(request,timeout = 10)
                                response = await stub.ModifyDataStore(request)
                            except Exception as e:
                                print("Transaction failed 2")
                                print(e)
                        continue
                    else:
                    #ROLLBACK logic
                    
                        p = TwoPhase[i[3]][1] 
                        #print("Executing Rollback for transaction", p)
                        #print(f"contact server {contact_server[TwoPhase[i[3]][2]]}")
                        #print(server_list_modified)
                        async with grpc.aio.insecure_channel(f'localhost:' + contact_server[TwoPhase[i[3]][2]]) as channel:
                            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                            request = transactions_pb2.TransactionRequest(
                                from_user=str(p[1]),
                                to_user=str(p[0]),
                                amount=p[2],
                                serverList=TwoPhase[i[3]][3],
                                isIntraShard=False,
                                isRollBack = True
                            )
                            try:
                                response = await stub.Transact(request)
                                print(response)
                            except Exception as e:
                                print("Transaction failed 2")
                                print(e)
                else:
                    if response.message == 'ABORTED':
                        #print("Cross Shard Transaction Aborted no rollback needed")
                        continue
                    else:
                        #print("Executing Rollback for transaction", i)
                        #print(f"contact server {contact_server[k]}")
                        async with grpc.aio.insecure_channel(f'localhost:' + contact_server[k]) as channel:
                            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                            request = transactions_pb2.TransactionRequest(
                            from_user=str(i[1]),
                            to_user=str(i[0]),
                            amount=i[2],
                            serverList=server_list_modified,
                            isIntraShard=False,
                            isRollBack = True
                        )
                            try:
                                response = await stub.Transact(request)
                            except Exception as e:
                                print("Transaction failed 2")
                                print(e)

                #await asyncio.sleep(1)
                
async def callPrintDB(num_clusters,server_list = server_list_global):
    s = 9//num_clusters
    server_list_clustered = [[] for _ in range(num_clusters)]
    for i in range(len(server_list)):
        #print((int(server_list[i][5])-1)//num_clusters)
        server_list_clustered[(int(server_list[i][5])-1)//num_clusters].append(server_list[i])
    return server_list_clustered
async def PrintBalance(server_list,server_list_clustered,cluster_size):
    print(cluster_size)
    server_list = server_list_global
    j = int(input("Enter client_id or -1 to exit"))
    #i = int(input("Enter server number "))
    #print(server_list_clustered)
    #cluster = server_list_clustered[j//cluster_size]
    
    
    while j!=-1:
        cluster = server_list_clustered[j//cluster_size]
        for i in cluster:
            async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
                stub = transactions_pb2_grpc.TransactionServiceStub(channel)
                request = transactions_pb2.GetBalanceRequest(client_id = j)
                try:
                    response = await stub.GetBalance(request)
                    print(f"Balance for server {i}: {response.balance}")
                    print()
                except Exception as e:
                    print(f"Failed to get balance for server {i}")
                    print(e)
        j = int(input("Enter client_id or -1 to exit"))
async def PrintDataStore():
    for i in server_list_global:
        async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
            stub = transactions_pb2_grpc.TransactionServiceStub(channel)
            request = transactions_pb2.GetLogRequest()
            try:
                response = await stub.GetLogs(request)
                print(f"DataStore for server {i}: {response.logs}")
                print()
            except Exception as e:
                print(f"Failed to get datastore for server {i}")
                print(f"Error{e}")
def Performance(start_time,end_time,num_transactions):
    print(f"Throughput: {num_transactions/(end_time-start_time)}")
    print(f"Latency: {(end_time-start_time)/num_transactions}")
async def main():
    #print("Hello World")
    file_path = './data/Test_Cases_-_Lab3.csv'
    df = pd.read_csv(file_path, header=None)
    df[0] = df[0].ffill().astype(int)
    unique_values = df[0].unique()
    dfs = {val: df[df[0] == val] for val in unique_values}
    print("Welcome to my PAXOS implementation")
    #print(dfs)
    print("Executing testcases")
    print("Enter number of clusters")
    num_clusters = int(input())
    for i in range(1,len(dfs)+1):
        x = input(f"Do you want to execute testcase {i} (Y/N)?")
        TwoPhase = {}
        #start_time = time.time()
        if x=='Y':
            dfs[i] = dfs[i].reset_index()
            transaction_list = dfs[i][1].tolist()
            transaction_list = [ast.literal_eval(t) for t in transaction_list]
            server_list = dfs[i][2][0].strip('[]').replace(" ", "").split(',')
            server_list = [f"50005{item[1:]}" for item in server_list]
            contact_server = dfs[i][3][0].strip('[]').replace(" ", "").split(',')
            contact_server = [f"50005{item[1:]}" for item in contact_server]    
            #print("transaction_list")
            #print(transaction_list)
            #print("server_list")
            #print(server_list)
            #print("contact_server")
            #print(contact_server)
            transaction_list = [list(x) for x in transaction_list]
            #num_clusters = 3
            #cluster_list_intra_shard = [[] for _ in range(num_clusters)]
            #cluster_list_cross_shard = [[] for _ in range(num_clusters)]
            cluster_list = [[] for _ in range(num_clusters)]
            #print(cluster_list)
            cluster_size = 3000//num_clusters
            #print(f"Cluster Size: {cluster_size}")
            sequence_number = 1
            for i in transaction_list:
                if i[0]//cluster_size != i[1]//cluster_size:
                    i.append(sequence_number)
                    cluster_list[i[0]//cluster_size].append(i)
                    cluster_list[i[1]//cluster_size].append(i)
                    sequence_number+=1
                    #cross_shard_transactions.append(i)
                else:
                    i.append(-1)
                    cluster_list[i[0]//cluster_size].append(i)
            #print(cluster_list)
            await catchup(server_list,num_clusters)
            start_time = time.time()
            tasks = [
                cluster_transact(cluster_list[i],server_list,i,
                                num_clusters,contact_server) for i in range(num_clusters)
            ]
            
            await asyncio.gather(*tasks)
            end_time = time.time()
            
            server_list_clustered = await callPrintDB(num_clusters)
            #print(f"Executed testcase {i}")
            print("Enter 1 for PrintBalance\nEnter 2 for PrintDatastore\nEnter 3 for Performance\nEnter 4 to Exit ")
            y = input()
            while(y!='4'):
                if(y=='1'):
                    await PrintBalance(server_list,server_list_clustered,cluster_size)
                elif(y=='2'):
                    await PrintDataStore()
                elif(y=='3'):
                    Performance(start_time,end_time,len(transaction_list))
                print("Enter 1 for PrintBalance\nEnter 2 for PrintDatastore\nEnter 3 for Performance\nEnter 4 to Exit ")
                y = input()
            #await printDB(server_list,server_list_clustered,cluster_size)   
            #await printDataStore()
    

asyncio.run(main())