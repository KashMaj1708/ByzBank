import subprocess

commands = [
    "python server.py 500051 S1",
    "python server.py 500052 S2",
    "python server.py 500053 S3",
    "python server.py 500054 S4",
    "python server.py 500055 S5",
    "python server.py 500056 S6",
    "python server.py 500057 S7",
    "python server.py 500058 S8",
    "python server.py 500059 S9",
    "python server.py 500060 S10",
    "python server.py 500061 S11",
    "python server.py 500062 S12"
]


processes = []
for cmd in commands:
    process = subprocess.Popen(["start", "cmd", "/k", cmd], shell=True)
    processes.append(process)


for process in processes:
    process.wait()
