import os

directory_path = './'  


x_values = [str(i) for i in range(500051, 500063)]


for x in x_values:
    file_name = f'client_balances_{x}.json'
    file_path = os.path.join(directory_path, file_name)
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f'Deleted: {file_path}')
        else:
            print(f'File not found: {file_path}')
    except Exception as e:
        print(f'Error deleting file {file_path}: {e}')