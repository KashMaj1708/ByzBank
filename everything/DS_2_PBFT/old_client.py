import grpc
import transactions_pb2
import transactions_pb2_grpc
import pandas as pd
import asyncio
import random

async def run_transaction(server,transactions,server_list):
    await asyncio.sleep(random.uniform(0,1))
    for transaction in transactions:
        async with grpc.aio.insecure_channel(f'localhost:{server}') as channel:
            stub = transactions_pb2_grpc.TransactionServiceStub(channel)

            for transaction in transactions:
                request = transactions_pb2.TransactionRequest(
                    from_user=transaction[0],
                    to_user=transaction[1],
                    amount=transaction[2]
                )
                response = await stub.Transact(request)  # Await the async RPC call
                print(f'Transaction success: {response.success}, Message: {response.message}')

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

async def main():

    file_path = '../data/lab1_Test.csv'
    df = pd.read_csv(file_path, header=None)
    df[0] = df[0].ffill().astype(int)
    unique_values = df[0].unique()
    dfs = {val: df[df[0] == val] for val in unique_values}
    for i in range(1,len(dfs)):
        dfs[i] = dfs[i].reset_index()
        if(input()=='1'):
            break
        transaction_list = dfs[i][1].tolist()
        server_list = dfs[i][2][0].strip('[]').replace(" ", "").split(',')
        server_list  = [switch_case(x) for x in server_list ]
        print(transaction_list)
        print(server_list)
        transaction_list_modified = { f'50005{i}' : [] for i in
                                      range(1, 6)}
        for item in transaction_list:
            parts = item.strip('()').replace(" ", "").split(',')
            print(parts)
            server1 = switch_case(parts[0])
            server2 = switch_case(parts[1])
            amount = int(parts[2])
            #print("TESTTT")
            print(server1,server2,amount)
            transaction_list_modified[server1].append((server1, server2, 
                                   amount))

        print(transaction_list_modified)
        tasks = [
            run_transaction(server,transaction_list_modified[server],server_list)
             for server in transaction_list_modified.keys() 
        ]
        await asyncio.gather(*tasks) 
asyncio.run(main())