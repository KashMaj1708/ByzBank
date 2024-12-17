import asyncio
import grpc
import project4_pb2
import project4_pb2_grpc
import random
import time
import pandas as pd
import numpy as np

class SmallBankBenchmark:
    def __init__(self, contact_servers, cluster_list):
        self.contact_servers = contact_servers
        self.cluster_list = cluster_list
        self.transaction_types = [
            'Balance',
            'Deposit',
            'Withdraw',
            'Transfer',
            'Amalgamate',
            'WriteCheck'
        ]
        self.client_ids = {
            0: list(range(1, 1000)),
            1: list(range(1001, 2000)),
            2: list(range(2001, 3000))
        }

    async def generate_transaction(self):
        cluster = random.randint(0, 2)
        client_a = random.choice(self.client_ids[cluster])
        trans_type = random.choice(self.transaction_types)
        
        if trans_type == 'Balance':
            return (client_a, client_a, 'Balance', 0)
        elif trans_type in ['Deposit', 'Withdraw', 'WriteCheck']:
            amount = random.randint(1, 100)
            return (client_a, client_a, trans_type, amount)
        elif trans_type == 'Transfer':
            if random.random() < 0.3:
                other_cluster = (cluster + 1) % 3
                client_b = random.choice(self.client_ids[other_cluster])
            else:
                client_b = random.choice([c for c in self.client_ids[cluster] if c != client_a])
            amount = random.randint(1, 100)
            return (client_a, client_b, trans_type, amount)
        elif trans_type == 'Amalgamate':
            if random.random() < 0.5:
                other_cluster = (cluster + 1) % 3
                client_b = random.choice(self.client_ids[other_cluster])
            else:
                client_b = random.choice([c for c in self.client_ids[cluster] if c != client_a])
            return (client_a, client_b, trans_type, 0)

    async def execute_transaction(self, transaction, delay=0):
        await asyncio.sleep(delay)
        client_a, client_b, trans_type, amount = transaction
        c1 = client_a // 1000
        c2 = client_b // 1000
        server_list_1 = [x for x in self.cluster_list[c1]]
        server_list_2 = [x for x in self.cluster_list[c2]] if c1 != c2 else server_list_1
        
        if trans_type == 'Balance':
            transaction_msg = f"({client_a}, {client_a}, 0)"
        elif trans_type == 'Deposit':
            transaction_msg = f"({client_a}, {client_a}, {amount})"
        elif trans_type == 'Withdraw':
            transaction_msg = f"({client_a}, {client_a}, -{amount})"
        elif trans_type == 'Transfer':
            transaction_msg = f"({client_a}, {client_b}, {amount})"
        elif trans_type == 'Amalgamate':
            transaction_msg = f"({client_a}, {client_b}, 0)"
        elif trans_type == 'WriteCheck':
            transaction_msg = f"({client_a}, {client_a}, -{amount})"
        
        try:
            if c1 == c2:
                async with grpc.aio.insecure_channel("localhost:"+self.contact_servers[c1]) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    response = await stub.ClientTransaction(
                        project4_pb2.ClientTransactionRequest(
                            name=str(client_a),
                            message=transaction_msg,
                            timestamp=int(time.time()),
                            signature=b'dummy_signature',
                            server_list=server_list_1,
                            isIntraShard=True
                        )
                    )
                    print(f"Intra-shard {trans_type} Transaction: {transaction_msg}, Response: {response.response}")
            else:
                async with grpc.aio.insecure_channel("localhost:"+self.contact_servers[c1]) as channel:
                    stub = project4_pb2_grpc.project4ServiceStub(channel)
                    response = await stub.ClientTransactionCrossShard(
                        project4_pb2.ClientTransactionCrossShardRequest(
                            name=str(client_a),
                            message=transaction_msg,
                            timestamp=int(time.time()),
                            server_list_1=server_list_1,
                            server_list_2=server_list_2,
                            signature_1=b'dummy_signature',
                            signature_2=b'dummy_signature'
                        )
                    )
                    print(f"Cross-shard {trans_type} Transaction: {transaction_msg}, Response: {response.response}")
        
        except Exception as e:
            print(f"Transaction Error: {e}")

    async def run_benchmark(self, num_transactions=100, concurrency=10):
        print("Starting SmallBank Benchmark")
        start_time = time.time()
        tasks = []
        for i in range(num_transactions):
            transaction = await self.generate_transaction()
            task = asyncio.create_task(self.execute_transaction(transaction, delay=i/100))
            tasks.append(task)
            if len(tasks) >= concurrency:
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)
        end_time = time.time()
        print("\nBenchmark Results:")
        print(f"Total Transactions: {num_transactions}")
        print(f"Total Execution Time: {end_time - start_time:.2f} seconds")
        print(f"Transactions per Second: {num_transactions / (end_time - start_time):.2f}")

async def main():
    cluster_list = [
        ["500051","500052","500053","500054"],
        ["500055","500056","500057","500058"],
        ["500059","500060","500061","500062"]
    ]
    contact_servers = ["500051", "500055", "500059"]
    benchmark = SmallBankBenchmark(contact_servers, cluster_list)
    await benchmark.run_benchmark(num_transactions=50, concurrency=5)

if __name__ == '__main__':
    asyncio.run(main())
