# Distributed Banking System with PBFT Consensus

A distributed banking system implementation using the Practical Byzantine Fault Tolerance (PBFT) consensus algorithm with support for cross-shard transactions and Byzantine fault tolerance.

## Project Overview

This project implements a distributed banking system that can handle:
- **Intra-shard transactions**: Transactions within the same shard
- **Cross-shard transactions**: Transactions across different shards
- **Triple cross-shard transactions**: Complex transactions involving three shards
- **Byzantine fault tolerance**: System continues to operate correctly even with malicious nodes
- **Digital signatures**: Cryptographic authentication for all transactions
- **Two-phase commit protocol**: For cross-shard transaction consistency

## Architecture

The system consists of:
- **12 servers** running on ports 500051-500062, organized into 3 clusters:
  - Cluster 1: Servers 500051-500054 (clients 1-999)
  - Cluster 2: Servers 500055-500058 (clients 1001-1999) 
  - Cluster 3: Servers 500059-500062 (clients 2001-2999)
- **Client applications** that can submit transactions
- **gRPC communication** between all components
- **TinyDB** for persistent storage of client balances
- **RSA digital signatures** for authentication

## Features

### Consensus Algorithm
- Implements PBFT (Practical Byzantine Fault Tolerance)
- Three-phase consensus: PrePrepare, Prepare, Commit
- Handles up to f Byzantine failures where f < n/3
- View change mechanism for leader replacement

### Transaction Types
- **Balance queries**: Check account balance
- **Deposits**: Add money to account
- **Withdrawals**: Remove money from account
- **Transfers**: Move money between accounts
- **Cross-shard transfers**: Transfer money across shards
- **Triple cross-shard transfers**: Complex multi-shard transactions

### Security Features
- RSA digital signatures for all messages
- Collision-resistant hashing (SHA-256)
- Signature verification for all requests
- Byzantine fault tolerance

## Prerequisites

- Python 3.7+
- pip (Python package manager)

## Installation

### Quick Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd distributed-banking-pbft
```

2. Run the setup script:
```bash
python scripts/setup.py
```

### Manual Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate gRPC Python files:
```bash
python -m grpc_tools.protoc --proto_path=config --python_out=generated --grpc_python_out=generated config/*.proto
```

3. Generate cryptographic keys:
```bash
python src/GenKeys.py
```

## Usage

### Starting the System

1. **Start all servers** (run in separate terminals or use the batch script):
```bash
# Option 1: Use the batch script (Windows)
python scripts/start_servers.py

# Option 2: Start servers manually
python src/server.py 500051 S1
python src/server.py 500052 S2
python src/server.py 500053 S3
python src/server.py 500054 S4
python src/server.py 500055 S5
python src/server.py 500056 S6
python src/server.py 500057 S7
python src/server.py 500058 S8
python src/server.py 500059 S9
python src/server.py 500060 S10
python src/server.py 500061 S11
python src/server.py 500062 S12
```

2. **Run the client application**:
```bash
python src/client.py
```

3. **Run examples**:
```bash
python examples/basic_usage.py
```

### Running Test Cases

The system includes test cases in the `data/test_cases/` directory:
- `Lab4_Testset_1.csv`: Basic test cases
- `Lab4_Testset_2.csv`: Advanced test cases

### Running Benchmarks

Run the SmallBank benchmark:
```bash
python src/SmallBank.py
```

## Project Structure

```
├── src/                     # Source code
│   ├── __init__.py         # Package initialization
│   ├── server.py           # Main server implementation
│   ├── client.py           # Client application
│   ├── SmallBank.py        # Benchmark implementation
│   ├── genKeyPair.py       # Cryptographic key generation
│   └── GenKeys.py          # Key generation script
├── config/                  # Configuration files
│   ├── project4.proto      # gRPC service definitions
│   ├── pbft.proto          # PBFT protocol definitions
│   ├── transactions.proto  # Transaction protocol definitions
│   └── servers.json        # Server configuration
├── generated/               # Generated gRPC Python code
│   ├── project4_pb2.py
│   ├── project4_pb2_grpc.py
│   ├── pbft_pb2.py
│   ├── pbft_pb2_grpc.py
│   ├── transactions_pb2.py
│   └── transactions_pb2_grpc.py
├── scripts/                 # Utility scripts
│   ├── setup.py            # Setup script
│   ├── start_servers.py    # Batch script to start all servers
│   └── del_data.py         # Data cleanup script
├── data/                    # Data directory
│   ├── test_cases/         # Test case files
│   │   ├── Lab4_Testset_1.csv
│   │   ├── Lab4_Testset_2.csv
│   │   └── Lab4_Testcases_Guide.pdf
│   ├── benchmarks/         # Benchmark data
│   └── client_balances_*.json  # Database files for each server
├── examples/                # Usage examples
│   └── basic_usage.py      # Basic usage example
├── docs/                    # Documentation
│   ├── CSE535_F24_Project4.pdf
│   └── Notes.txt
├── keys/                    # Cryptographic keys
│   ├── client_keys/        # Client private/public keys
│   └── server_keys/        # Server private/public keys
├── tests/                   # Test files
├── README.md               # This file
├── requirements.txt        # Python dependencies
└── .gitignore             # Git ignore rules
```

## Configuration

### Server Configuration
- **Ports**: 500051-500062
- **Client ranges**: 
  - Cluster 1: Clients 1-999
  - Cluster 2: Clients 1001-1999
  - Cluster 3: Clients 2001-2999
- **Initial balance**: 10 units per client

### Byzantine Fault Tolerance
- System can tolerate up to f Byzantine failures where f < n/3
- For 12 servers, can handle up to 3 Byzantine failures
- Byzantine servers are specified in test cases

## Transaction Format

Transactions are represented as tuples:
- **Intra-shard**: `(from_client, to_client, amount)`
- **Cross-shard**: `(from_client, to_client, amount)`
- **Triple cross-shard**: `(client1, client2, client3, amount)`

## Error Handling

The system includes comprehensive error handling for:
- Network failures
- Byzantine node behavior
- Invalid signatures
- Insufficient funds
- Timeout scenarios
- Cross-shard transaction rollbacks

## Performance

The system includes a SmallBank benchmark that measures:
- Transaction throughput
- Latency
- Success rates
- Cross-shard vs intra-shard performance

## GitHub Repository

This project is structured for easy GitHub hosting with:
- Clear directory organization
- Comprehensive documentation
- Proper `.gitignore` for Python projects
- Example usage scripts
- Configuration management

### Repository Structure Benefits
- **`src/`**: Clean source code organization
- **`config/`**: Centralized configuration files
- **`generated/`**: Auto-generated files (excluded from git)
- **`examples/`**: Usage examples for new users
- **`docs/`**: Project documentation
- **`scripts/`**: Utility and setup scripts

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests if applicable
5. Commit your changes (`git commit -m 'Add some amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Development

### Setting up for development
```bash
git clone <repository-url>
cd distributed-banking-pbft
python scripts/setup.py
```

### Running tests
```bash
python -m pytest tests/
```

### Code style
This project follows PEP 8 style guidelines.

## License

This project is part of CSE535 - Distributed Systems coursework.

## Authors

- **Kash Majumdar** - *Initial work* - [kashmaj1708](https://github.com/kashmaj1708)

## Course Information

- **Course**: CSE535 - Distributed Systems
- **Institution**: Stony Brook University
- **Project**: Project 4 - PBFT Implementation
- **Semester**: Fall 2024

## Acknowledgments

- Stony Brook University CSE Department
- Professor and TAs for CSE535
- PBFT algorithm by Castro and Liskov
