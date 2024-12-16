import asyncio
import grpc
import pbft_pb2
import pbft_pb2_grpc
import time
from genKeyPair import sign_message
import pandas as pd

def switch_case(value):
        match value:
            case "S1":
                return "500051"
            case "S2":
                return "500052"
            case "S3":
                return "500053"
            case "S4":
                return "500054"
            case "S5":
                return "500055"
            case "S6":
                return "500056"
            case "S7":
                return "500057"

count = 1           

universal_server_list = ["500051","500052","500053","500054","500055","500056","500057"]
async def transact(client,transactions,server_list,byzantine_list,view_number,primary_server,timeout=24):
    print(f"client {client} transactions {transactions} server_list {server_list} byzantine_list {byzantine_list} view_number {view_number} primary_server {primary_server} timeout {timeout}")
    try:
        
        responses = []
        timeout = timeout
        for i in range(len(transactions)):
            async with grpc.aio.insecure_channel("localhost:"+primary_server) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                current_time = int(time.time())
                try:
                    #print(f"./keys/client_keys/client_{client}_private_key.pem")
                    signature = sign_message(f"./keys/client_keys/client_{client}_private_key.pem",client)
                except FileNotFoundError:
                    print(f"Error: Private key file not found for {client}")
                    return None
                except Exception as sign_error:
                    print(f"Signature creation error for {client}: {sign_error}")
                    return None
               # print("CHECK")
                #print(transactions[i])        
                request = pbft_pb2.ClientTransactionRequest(
                    name=client,
                    message=str(transactions[i]),
                    timestamp=current_time,
                    signature=signature,
                    server_list = server_list,
                    byzantine_list = byzantine_list if not byzantine_list[0] == None else [],
                    view_number = view_number
                )
                
                try:
                    #rpc_task = asyncio.create_task(stub.ClientTransaction(request))
                    async def make_rpc_call():
                        #print("CHECK 2")
                        response =  await stub.ClientTransaction(request)
                        return response
                    try:
                        t1 = time.time()
                        #print
                        #print("CHECK 3")
                        response = await asyncio.wait_for(make_rpc_call( ), timeout=timeout)
                        #print(f"finished RPC call {str(transactions[i])} in time {time.time()-t1}")
                        #count+=1
                        #print("Response")
                        #print(f"{transactions[i]} - {response}")
                        responses.append(response)
                    except asyncio.TimeoutError:
                        #print("TIMEOUT")
                        responses.append(None)
                        return "VC"
                    except grpc.RpcError as e:
                        #print(f"RPC failed for {client} with {e.code()}: {e.details()}")
                        responses.append(None)
                        continue
                
                except grpc.RpcError as e:
                    print(f"RPC failed for {client} with {e.code()}: {e.details()}")
                    responses.append(None)
        
        return responses
        
    except Exception as e:
        print(f"Unexpected error in transaction to {client}: {e}")
        return None
async def transact_all_servers(client,transaction,server_list,byzantine_list,view_number,primary):
    #print(f"Called transact_all_servers for client {client} with primary {primary}") 
   # print(f"Transaction: {transaction}")
    result = await transact(client,transaction,server_list,byzantine_list,view_number,primary)
    return result

async def transact_vc(client,transactions,server_list,byzantine_list,view_number):
    #print(f"Called transact_vc for client {client}")
    try:
        responses = []
        timeout = 10
        tasks = [transact_all_servers(client,transactions,
                        server_list,byzantine_list,view_number,primary) 
                         for primary in server_list]
        results = await asyncio.gather(*[
            asyncio.create_task(task) for task in tasks
    ])
        return 'VC' not in results
    except grpc.RpcError as e:
                    print(f"RPC failed for {client} with {e.code()}: {e.details()}")
                    responses.append(None)

async def send_transactions_to_multiple_servers(
        transaction_list_modified,server_list,byzantine_list,view_number,timeout=24):
    primary_server = "50005"+str(view_number)
    tasks = [
        transact(client,transaction_list_modified[client],
                 server_list,byzantine_list,view_number,primary_server,timeout) 
        for client in transaction_list_modified.keys()
    ]
    results = await asyncio.gather(*[
        asyncio.create_task(task) for task in tasks
        
    ])
    #print("Results")
    #print(results)
    #print('VC' in results)
    # print()
    return 'VC' not in results

async def view_change_protocol(
     transaction_list_modified,server_list,byzantine_list,view_number):
     #print("Called view change protocol")
    tasks = [
         transact_vc(client,transaction_list_modified[client],
                  server_list,byzantine_list,view_number) 
         for client in transaction_list_modified.keys()
     ]
    results = await asyncio.gather(*[
         asyncio.create_task(task) for task in tasks
     ])
    return False not in results
def reset_all_servers(server_list):
    reset_results = {}
    for server in server_list:
        try:
            with grpc.insecure_channel(f'localhost:{server}') as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                request = pbft_pb2.ResetServerStateRequest(
                )
                response = stub.ResetServerState(request)
                reset_results[server] = {
                    'reset_successful': response.reset_successful,
                    'server_port': response.server_port
                }
                print(f"Reset server {server}: {response.reset_successful}")
        
        except grpc.RpcError as e:
            print(f"RPC error resetting server {server}: {e}")
            reset_results[server] = {
                'reset_successful': False, 
                'error': str(e)
            }
        except Exception as e:
            print(f"Unexpected error resetting server {server}: {e}")
            reset_results[server] = {
                'reset_successful': False, 
                'error': str(e)
            }
    return reset_results
def get_server_balances(server):
    try:
        with grpc.insecure_channel(server) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            request = pbft_pb2.BalancesRequest(
            )
            response = stub.GetServerBalances(request)
            balances = dict(response.balances)       
            return balances
    
    except grpc.RpcError as e:
        print(f"RPC error getting balances from server {server}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error getting balances from server {server}: {e}")
        return None
def PrintLog():
    print("Enter Server")
    server = switch_case(input())
    print(server)
    with grpc.insecure_channel("localhost:"+server) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        request = pbft_pb2.PrintLogRequest(
                )
        response = stub.PrintLog(request)
        #print("YOUR LOG")
        print(response.log)
def PrintDB(view_number):
    sorted_balances = dict(sorted(get_server_balances(f"localhost:50005{view_number}").items()))
    print(sorted_balances)
def PrintStatus(server_list):
    s = int(input("Enter sequence number"))
    for server in server_list:
        with grpc.insecure_channel("localhost:"+server) as channel:
            stub = pbft_pb2_grpc.PBFTServiceStub(channel)
            request = pbft_pb2.PrintStatusRequest(
                sequence_number = s
                    )
            response = stub.PrintStatus(request)
            print(response.status)
def PrintView(view_number):
    with grpc.insecure_channel("localhost:50005"+str(view_number+2)) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        request = pbft_pb2.PrintViewRequest(
                )
        response = stub.PrintView(request)
        print(response.view_logs)
def PrintPerformance(t1,n,view_number):
    print("time")
    print(f" Throughput: {(n*view_number)/(time.time()-t1)}")
    print(f" Latency: {(time.time()-t1)/(n*view_number)}")
def menu(server_list,view_number,t1,n):
    print("<---------- MENU------------->")
    print("Enter 1 for PrintLog") 
    print("Enter 2 for PrintDB")
    print("Enter 3 for PrintStatus")
    print("Enter 4 for PrintView")
    print("Enter 5 for Performance Metrics")
    print("Enter 7 to EXIT")
    x = input()
    while(x!='7'):
        if x == '1':
            PrintLog()
        elif x == '2':
            PrintDB(view_number)
        elif x == '3':
            PrintStatus(server_list)
        elif x == '4':
            PrintView(view_number)
        elif x == '5':
            PrintPerformance(t1,n,view_number)
        print("<---------- MENU------------->")
        print("Enter 1 for PrintLog") 
        print("Enter 2 for PrintDB")
        print("Enter 3 for PrintStatus")
        print("Enter 4 for PrintView")
        print("Enter 5 for Performance Metrics")
        print("Enter 7 to EXIT")
        x = input()
def run():
    
    async def main():
        
        file_path = './data/dataset.csv'
        df = pd.read_csv(file_path, header=None)
        #print(df)
        #print("Dont enter 1")
        df.drop(df.columns[0], axis=1)
        df[0] = df[0].ffill().astype('int')
        unique_values = df[0].unique()
        dfs = {val: df[df[0] == val] for val in unique_values}
        for i in range(1,len(dfs)+1):
            print("Executing test case ",i)
            print("Enter Y to continue or N to skip")
            p = input()
            if p == 'N' or p == 'n':
                continue
            view_number = 1
            dfs[i] = dfs[i].reset_index()
            #if(input()=='1'):
                #break
            transaction_list = dfs[i][1].tolist()
            n = len(transaction_list)
            server_list = dfs[i][2][0].strip('[]').replace(" ", "").split(',')
            server_list  = [switch_case(x) for x in server_list ]
            byzantine_list = dfs[i][3][0].strip('[]').replace(" ", "").split(',')
            byzantine_list = [switch_case(x) for x in byzantine_list ]
            transaction_list_modified = { chr(i) : [] for i in
                                        range(ord('A'), ord('J')+1)}
           
            for item in transaction_list:
                parts = item.strip('()').replace(" ", "").split(',')
                #print(parts)
                sender = parts[0]
                receiver = parts[1]
                amount = int(parts[2])
                #print(sender,receiver,amount)
                transaction_list_modified[sender].append((sender, receiver, 
                                    amount))
            #print("Transaction List Modified:")
            #print(transaction_list_modified ) 
            t1 = time.time()      
            if i == 10 or i==6:
                with grpc.insecure_channel("localhost:500051") as channel:
                    stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                    request = pbft_pb2.ChangeServerTimeoutRequest(
                    )
                    response = stub.ChangeServerTimeout(request)
                    #print(f"Reset server 500051: {response.reset_successful}")
                result = await send_transactions_to_multiple_servers(
                transaction_list_modified,server_list,byzantine_list,view_number,34)
            else:
                result = await send_transactions_to_multiple_servers(
                transaction_list_modified,server_list,byzantine_list,view_number)
            #print("Normal case")
            #print(result)
            
            #print(result)
            #await asyncio.sleep(10)
            #result = await view_change_protocol(transaction_list_modified,
                            #server_list,byzantine_list,view_number+1)
            await asyncio.sleep(5)
            #print(f"View Change karu kya? {result}")
            while not result and view_number <= 7:
                print("VIEW CHANGE")
                view_number += 1
                result = await view_change_protocol(transaction_list_modified, server_list, byzantine_list, view_number % 7)
                await asyncio.sleep(12)
                #print("URAAA",result)
                #result = True

            #print(result)
            #print("Balances:")
            #sorted_balances = dict(sorted(get_server_balances(f"localhost:50005{view_number}").items()))
            #print(sorted_balances)
            menu(server_list,view_number,t1,n)
            reset_results = reset_all_servers(universal_server_list)
            #print(reset_results)
            
    asyncio.run(main())

def main():
    run()
if __name__ == '__main__':
    main()