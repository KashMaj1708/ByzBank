import subprocess
import sys
import argparse
def start_servers(start_port=500051, end_port=500057):
    for port in range(start_port, end_port + 1):
        try:
            command = f'python server.py {port}'
            subprocess.Popen(f'start cmd /k "{command}"', shell=True)            
            print(f"Started server on port {port}")
        except Exception as e:
            print(f"Failed to start server on port {port}: {e}")
def main():
    parser = argparse.ArgumentParser(description='Start Multiple PBFT Servers')
    parser.add_argument('--start', type=int, default=500051, 
                        help='Starting port number (default: 50051)')
    parser.add_argument('--end', type=int, default=500057, 
                        help='Ending port number (default: 50057)')
    args = parser.parse_args()
    start_servers(args.start, args.end)

if __name__ == '__main__':
    main()