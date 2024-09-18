import socket
import random
import time
import json
import threading
import logging
from datetime import datetime

# Logging 설정
logging.basicConfig(filename='worker.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def matrix_multiplication(matrix_a, matrix_b, i, j):
    """ 주어진 인덱스 (i, j)에서 행렬 곱셈을 수행합니다. """
    result = sum(matrix_a[i][k] * matrix_b[k][j] for k in range(len(matrix_a)))
    return result

def worker_logic(client_socket, matrix_a, matrix_b):
    """ 워커 노드의 주요 작업 로직을 처리합니다. """
    tasks = []
    start_time = time.time()
    
    while True:
        try:
            # 서버로부터 작업을 수신합니다.
            task = client_socket.recv(1024).decode()
            if not task:
                break  # 작업이 없으면 루프 종료.

            i, j = map(int, task.split(','))

            # 서버로부터 행렬 데이터 수신
            matrix_a = json.loads(client_socket.recv(65536).decode())
            matrix_b = json.loads(client_socket.recv(65536).decode())

            # 랜덤한 시간(1~3초) 동안 작업을 수행합니다.
            wait_time = random.randint(1, 3)
            time.sleep(wait_time)
            
            # 작업 성공/실패 확률
            if random.random() < 0.8:
                result = matrix_multiplication(matrix_a, matrix_b, i, j)
                client_socket.sendall(str(result).encode())
                logging.info(f"Task ({i}, {j}) completed with result {result}.")
            else:
                client_socket.sendall("FAIL".encode())
                logging.info(f"Task ({i}, {j}) failed.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            break

    # 결과 및 성과를 출력
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Worker completed in {elapsed_time:.2f} seconds.")

def connect_to_server():
    """ 서버에 연결하고 작업을 수행합니다. """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('34.68.170.234', 5000))

    while True:
        try:
            # 서버로부터 작업을 수신합니다.
            task = client_socket.recv(1024).decode()
            if not task:
                break

            i, j = map(int, task.split(','))
            # 서버로부터 행렬 데이터 수신
            matrix_a = json.loads(client_socket.recv(65536).decode())
            matrix_b = json.loads(client_socket.recv(65536).decode())
            
            # 워커 로직을 처리합니다.
            worker_logic(client_socket, matrix_a, matrix_b)

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            break

    client_socket.close()

if __name__ == "__main__":
    connect_to_server()
