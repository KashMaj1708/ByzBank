import asyncio
import grpc
import project4_pb2
import project4_pb2_grpc
import time
from genKeyPair import sign_message
import pandas as pd
import ast
import random
cluster_list = [["500051","500052","500053","500054"],["500055","500056","500057","500058"],["500059","500060","500061","500062"]]
async def call_transact(transaction,server_list,byzantine_list,contact_server_list,delay):
    while True:
        try:
            await asyncio.wait_for(transact(transaction,server_list,byzantine_list,
                                            contact_server_list,delay),timeout=10)
            break
        except asyncio.TimeoutError:
            print(f"Timeout")
            continue
async def transact(transaction,server_list,byzantine_list,contact_server_list,delay):
    #print(f"Dalay is {delay}")
    await asyncio.sleep(delay)
    print(type(transaction))
    if len(transaction)>3:
        c1 = transaction[0]//1000
        c2 = transaction[1]//1000
        c3 = transaction[2]//1000
        try:
            
            server_list_1 = [x for x in server_list if x in cluster_list[c1]]
            server_list_2 = [x for x in server_list if x in cluster_list[c2]]
            server_list_3 = [x for x in server_list if x in cluster_list[c3]]
            print(f"Sending triple cross shard transaction {transaction}")
            async with grpc.aio.insecure_channel("localhost:"+contact_server_list[c1]) as channel:
                stub = project4_pb2_grpc.project4ServiceStub(channel)
                current_time = int(time.time())
                try:
                    signature_1 = sign_message(f"./keys/client_keys/client_{str(transaction[0])}_private_key.pem",str(transaction[0]))
                    signature_2 = sign_message(f"./keys/client_keys/client_{str(transaction[1])}_private_key.pem",str(transaction[1]))
                    signature_3 = sign_message(f"./keys/client_keys/client_{str(transaction[2])}_private_key.pem",str(transaction[2]))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(transaction[0])}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(transaction[0])}: {sign_error}")
                    return None
               
                request = project4_pb2.ClientTransactionTripleCrossShardRequest(
                    name=str(transaction[0]),
                    message=str(transaction),
                    timestamp=current_time,
                    #signature=signature,
                    server_list_1 = server_list_1,
                    server_list_2 = server_list_2,
                    server_list_3 = server_list_3,
                    byzantine_list = list(byzantine_list if byzantine_list and byzantine_list[0] is not None else []),
                    isIntraShard = False,
                    contact_server_list = contact_server_list,
                    signature_1 = signature_1,
                    signature_2 = signature_2,
                    isPendingTrans = False,
                    #cross_shard_request = None
                )
               
                response = await stub.ClientTransactionTripleCrossShard(request)
                print(response)
        except Exception as e:
            print(f"Error in cross shard transaction: {e}")
            return None
        
    print(f" transaction {transaction} server_list {server_list} byzantine_list {byzantine_list} contact_server_list = {contact_server_list}")
    print(time.time())
    c1 = transaction[0]//1000
    c2 = transaction[1]//1000
    print(f"c1 is {c1} c2 is {c2}")
    if c1==c2:
        print("Intra shard transaction")
        try:
            server_list_cluster = [x for x in server_list if x in cluster_list[c1]]
            print(f"Server list cluster is {server_list_cluster}")
            async with grpc.aio.insecure_channel("localhost:"+contact_server_list[c1]) as channel:
                stub = project4_pb2_grpc.project4ServiceStub(channel)
                current_time = int(time.time())
                try:
                    signature = sign_message(f"./keys/client_keys/client_{str(transaction[0])}_private_key.pem",str(transaction[0]))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(transaction[0])}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(transaction[0])}: {sign_error}")
                    return None
               
                request = project4_pb2.ClientTransactionRequest(
                    name=str(transaction[0]),
                    message=str(transaction),
                    timestamp=current_time,
                    signature=signature,
                    server_list = server_list_cluster,
                    byzantine_list = byzantine_list if byzantine_list and byzantine_list[0] is not None else [],
                    isIntraShard = True,
                    isRollBack = False,
                    contact_server_list = contact_server_list,
                    isPendingTrans = False
                )
                print(f"Request is {request}")
                
                response = await stub.ClientTransaction(request)
                print(response)
        except Exception as e:
            print(f"Error in intrashard transaction: {e}")
            return None
    else:
        try:
            
            server_list_1 = [x for x in server_list if x in cluster_list[c1]]
            server_list_2 = [x for x in server_list if x in cluster_list[c2]]
            print(f"Sending cross shard transaction {transaction}")
            async with grpc.aio.insecure_channel("localhost:"+contact_server_list[c1]) as channel:
                stub = project4_pb2_grpc.project4ServiceStub(channel)
                current_time = int(time.time())
                try:
                    signature_1 = sign_message(f"./keys/client_keys/client_{str(transaction[0])}_private_key.pem",str(transaction[0]))
                    signature_2 = sign_message(f"./keys/client_keys/client_{str(transaction[1])}_private_key.pem",str(transaction[1]))
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {str(transaction[0])}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {str(transaction[0])}: {sign_error}")
                    return None
               
                request = project4_pb2.ClientTransactionCrossShardRequest(
                    name=str(transaction[0]),
                    message=str(transaction),
                    timestamp=current_time,
                    #signature=signature,
                    server_list_1 = server_list_1,
                    server_list_2 = server_list_2,
                    byzantine_list = list(byzantine_list if byzantine_list and byzantine_list[0] is not None else []),
                    isIntraShard = False,
                    contact_server_list = contact_server_list,
                    signature_1 = signature_1,
                    signature_2 = signature_2,
                    isPendingTrans = False,
                    #cross_shard_request = None
                )
               
                response = await stub.ClientTransactionCrossShard(request)
                print(response)
        except Exception as e:
            print(f"Error in cross shard transaction: {e}")
            return None
        
async def PrintDatastore():
    for i in range(500051,500063):
        async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
            stub = project4_pb2_grpc.project4ServiceStub(channel)
            request = project4_pb2.GetDataStoreRequest()
            try:
                response = await stub.GetDataStore(request)
                print(f"Datastore for server {i}: {response.datastore}")
                print()
            except Exception as e:
                print(f"Failed to get datastore for server {i}")
                print(e)
async def run():
    print("Running")
    file_path = './data/Lab4_Testset_2.csv'
    df = pd.read_csv(file_path, header=None)
    df[0] = df[0].ffill().astype(int)
    unique_values = df[0].unique()
    dfs = {val: df[df[0] == val] for val in unique_values}
    print("Welcome to my PROJECT 4 implementation")
    print("Executing testcases")
    print(dfs)
    for i in range(1,len(dfs)+1):
        x = input(f"Do you want to execute testcase {i} (Y/N)?")
        if x=='Y' or x == 'y':
            dfs[i] = dfs[i].reset_index()
            transaction_list = dfs[i][1].tolist()
            transaction_list = [ast.literal_eval(t) for t in transaction_list]
            server_list = dfs[i][2][0].strip('[]').replace(" ", "").split(',')
            server_list = [str(500050+int(item[1:])) for item in server_list]
            byzantine_list = dfs[i][4][0].strip('[]').replace(" ", "").split(',')
            byzantine_list = [str(500050+int(item[1:])) for item in byzantine_list]
            print(f"Byzantine list is {byzantine_list}")
            contact_server_list = dfs[i][3][0].strip('[]').replace(" ", "").split(',')
            contact_server_list = [str(500050+int(item[1:])) for item in contact_server_list]
            print(f"Contact server list is {contact_server_list}")
            
            delay = 0.0
            tasks = [call_transact(transaction,server_list,byzantine_list,contact_server_list,delay+i/10) 
                     for i,transaction in enumerate(transaction_list)]
            await asyncio.gather(*tasks)
            await PrintDatastore()
            j = int(input("Enter client_id or -1 to exit"))
            while j!=-1:
                c = j//1000
                for i in cluster_list[c]:
                    async with grpc.aio.insecure_channel(f'localhost:{i}') as channel:
                        stub = project4_pb2_grpc.project4ServiceStub(channel)
                        request = project4_pb2.GetBalanceRequest(client_id = j)
                        try:
                                response = await stub.GetBalance(request)
                                print(f"Balance for server {i}: {response.balance}")
                                print()
                        except Exception as e:
                                print(f"Failed to get balance for server {1}")
                                print(e)
                j = int(input("Enter client_id or -1 to exit"))
    
if __name__ == '__main__':
    asyncio.run(run())