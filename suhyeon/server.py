import socket
import threading
import random
import time
import json
import logging
from queue import Queue
from datetime import datetime

# Logging 설정
logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 상수 설정
MATRIX_SIZE = 1000
NUM_WORKERS = 4

# 두 개의 랜덤 행렬 생성
matrix_a = [[random.randint(1, 10) for _ in range(MATRIX_SIZE)] for _ in range(MATRIX_SIZE)]
matrix_b = [[random.randint(1, 10) for _ in range(MATRIX_SIZE)] for _ in range(MATRIX_SIZE)]
result_matrix = [[0 for _ in range(MATRIX_SIZE)] for _ in range(MATRIX_SIZE)]

# 작업 큐와 각 워커의 작업 수를 관리하기 위한 설정
task_queue = Queue()
worker_tasks = {}
worker_status = {}  # 변경: 각 워커의 상태를 추적합니다.

for i in range(MATRIX_SIZE):
    for j in range(MATRIX_SIZE):
        task_queue.put((i, j))

start_time = time.time()
system_clock = 0

def system_clock_func():
    """ 시스템 클록을 1초마다 업데이트합니다. """
    global system_clock
    while not task_queue.empty():
        system_clock = int(time.time() - start_time)
        time.sleep(1)

def distribute_tasks(worker_id, conn):
    """ 워커에게 작업을 배분하고 결과를 수신합니다. """
    global worker_tasks
    worker_status[worker_id] = {"waiting_time": [], "completed_tasks": 0, "failed_tasks": 0}
    while not task_queue.empty() and worker_tasks[worker_id] < 10:
        task = task_queue.get()
        i, j = task
        try:
            conn.sendall(f"{i},{j}".encode())
            conn.sendall(json.dumps(matrix_a).encode())
            conn.sendall(json.dumps(matrix_b).encode())
            
            worker_tasks[worker_id] += 1

            start_task_time = time.time()
            response = conn.recv(1024).decode()
            end_task_time = time.time()
            
            waiting_time = end_task_time - start_task_time
            worker_status[worker_id]["waiting_time"].append(waiting_time)
            
            if response == 'FAIL':
                worker_status[worker_id]["failed_tasks"] += 1
                task_queue.put((i, j))  # 실패한 작업을 재배치합니다.
                logging.info(f"Task ({i}, {j}) failed. Reassigning.")
            else:
                result = int(response)
                result_matrix[i][j] = result
                worker_status[worker_id]["completed_tasks"] += 1
                logging.info(f"Task ({i}, {j}) completed with result {result}.")
            
            worker_tasks[worker_id] -= 1

        except (ConnectionResetError, BrokenPipeError) as e:
            logging.error(f"Connection error with Worker {worker_id}: {e}")
            break

    # 워커 성과를 로그로 출력합니다.
    end_time = time.time()
    total_time = end_time - start_time
    avg_waiting_time = sum(worker_status[worker_id]["waiting_time"]) / len(worker_status[worker_id]["waiting_time"]) if worker_status[worker_id]["waiting_time"] else 0
    logging.info(f"Worker {worker_id} completed with {worker_status[worker_id]['completed_tasks']} tasks completed, {worker_status[worker_id]['failed_tasks']} tasks failed, average waiting time {avg_waiting_time:.2f} seconds, and total time {total_time:.2f} seconds.")

def handle_worker_response(worker_id, conn, addr):
    """ 워커의 응답을 처리합니다. """
    logging.info(f"Connected to Worker {worker_id} at {addr}")
    worker_tasks[worker_id] = 0
    distribute_tasks(worker_id, conn)
    conn.close()

def start_server():
    """ 서버를 시작하고 워커의 연결을 기다립니다. """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', 5000))
    server_socket.listen(NUM_WORKERS)

    logging.info("Server started, waiting for workers...")

    # 시스템 클록 스레드 시작
    threading.Thread(target=system_clock_func).start()

    worker_id = 0
    while worker_id < NUM_WORKERS:
        conn, addr = server_socket.accept()
        worker_id += 1
        threading.Thread(target=handle_worker_response, args=(worker_id, conn, addr)).start()

    server_socket.close()

if __name__ == "__main__":
    start_server()
