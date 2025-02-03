import os

def check_data_path():
    data_path = os.getenv('DATA_PATH')
    if data_path:
        print(f"DATA_PATH: {data_path}")
    else:
        print("DATA_PATH não está definida.")

if __name__ == "__main__":
    check_data_path()