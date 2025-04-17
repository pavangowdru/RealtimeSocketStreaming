import socket
import json
import pandas as pd
import time

def send_data_over_socket(file_path, host='localhost', port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host,port))
    s.listen(1)
    print(f"Listening for connection on {host}:{port}")

    conn,addr = s.accept()
    print(f"Connection from {addr}")

    last_sent_index = 0
    try:
        with open(file_path, 'r') as file:
            for _ in range(last_sent_index):
                next(file)

            records = []
            for line in file:
                records.append(json.loads(line))
                if(len(records) == chunk_size ):
                    chunk = pd.DataFrame(records)
                    print(chunk)
                    for record in chunk.to_dict(orient='records'):
                        serialize_data = json.dumps(record).encode('utf-8')
                        conn.send(serialize_data + b'\n')
                        time.sleep(5)
                        last_sent_index += chunk_size
                    
                    records = []
    except(BrokenPipeError, ConnectionResetError):
        print("Client has disconnected")
    finally:
        #conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    send_data_over_socket("datasets/movie_review.json")        


