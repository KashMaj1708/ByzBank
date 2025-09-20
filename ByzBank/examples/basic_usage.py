#!/usr/bin/env python3
"""
Basic usage example for the Distributed Banking System with PBFT Consensus.

This example demonstrates how to:
1. Start the system
2. Submit transactions
3. Query balances
4. Handle cross-shard transactions
"""

import asyncio
import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from client import transact, PrintDatastore

async def example_intra_shard_transaction():
    """Example of an intra-shard transaction (within same cluster)."""
    print("=== Intra-Shard Transaction Example ===")
    
    # Transaction: Transfer 5 units from client 100 to client 200 (both in cluster 1)
    transaction = (100, 200, 5)
    server_list = ["500051", "500052", "500053", "500054"]
    byzantine_list = []
    contact_server_list = ["500051", "500055", "500059"]
    
    print(f"Transaction: {transaction}")
    print(f"Server list: {server_list}")
    
    try:
        await transact(transaction, server_list, byzantine_list, contact_server_list, 0)
        print("✓ Transaction completed successfully")
    except Exception as e:
        print(f"✗ Transaction failed: {e}")

async def example_cross_shard_transaction():
    """Example of a cross-shard transaction (across different clusters)."""
    print("\n=== Cross-Shard Transaction Example ===")
    
    # Transaction: Transfer 3 units from client 100 (cluster 1) to client 1500 (cluster 2)
    transaction = (100, 1500, 3)
    server_list = ["500051", "500052", "500053", "500054", "500055", "500056", "500057", "500058"]
    byzantine_list = []
    contact_server_list = ["500051", "500055", "500059"]
    
    print(f"Transaction: {transaction}")
    print(f"Server list: {server_list}")
    
    try:
        await transact(transaction, server_list, byzantine_list, contact_server_list, 0)
        print("✓ Cross-shard transaction completed successfully")
    except Exception as e:
        print(f"✗ Cross-shard transaction failed: {e}")

async def example_balance_query():
    """Example of querying account balance."""
    print("\n=== Balance Query Example ===")
    
    import grpc
    from project4_pb2_grpc import project4ServiceStub
    from project4_pb2 import GetBalanceRequest
    
    client_id = 100
    cluster = client_id // 1000
    contact_server = 500051 + cluster * 4  # Calculate contact server
    
    try:
        async with grpc.aio.insecure_channel(f'localhost:{contact_server}') as channel:
            stub = project4ServiceStub(channel)
            request = GetBalanceRequest(client_id=client_id)
            response = await stub.GetBalance(request)
            print(f"Balance for client {client_id}: {response.balance}")
    except Exception as e:
        print(f"✗ Balance query failed: {e}")

async def main():
    """Main example function."""
    print("Distributed Banking System - Basic Usage Example")
    print("=" * 50)
    
    # Note: Make sure servers are running before executing these examples
    print("Note: Make sure to start the servers first using:")
    print("python scripts/start_servers.py")
    print()
    
    try:
        await example_intra_shard_transaction()
        await example_cross_shard_transaction()
        await example_balance_query()
        
        print("\n=== Data Store Status ===")
        await PrintDatastore()
        
    except KeyboardInterrupt:
        print("\nExample interrupted by user")
    except Exception as e:
        print(f"Example failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
