# master.py
import socket
import threading
import json
import numpy as np
import logging
import time
from queue import Queue

# 설정 파일 로드
with open('config.json', 'r') as f:
    config = json.load(f)

MASTER_IP = config['master']['ip']
MASTER_PORT = config['master']['port']
WORKERS = config['workers']

# 로그 설정
logging.basicConfig(filename='Master.txt',
                    level=logging.INFO,
                    format='[%(asctime)s] %(message)s',
                    datefmt='%H:%M:%S')

# 행렬 크기
MATRIX_SIZE = 1000

# 작업 큐
task_queue = Queue()
result_matrix = np.zeros((MATRIX_SIZE, MATRIX_SIZE), dtype=int)

# 시스템 클럭 시작 시간
start_time = time.time()

# 워커 상태 저장
workers_status = {}
workers_lock = threading.Lock()

# 작업 할당 함수
def assign_tasks(conn, addr, worker_id):
    global task_queue, result_matrix
    logging.info(f"Worker{worker_id} connected from {addr}")
    try:
        while True:
            task = task_queue.get()
            if task is None:
                break  # 종료 신호
            # 작업을 전송 (i, j, A[i][j], B[j][k])
            i, j = task
            data = json.dumps({'type': 'task', 'i': i, 'j': j, 'A_row': A[i].tolist(), 'B_col': B[:,j].tolist()})
            conn.sendall(data.encode())
            logging.info(f"Task assigned to Worker{worker_id}: C[{i},{j}]")
    except Exception as e:
        logging.error(f"Connection with Worker{worker_id} lost: {e}")
    finally:
        conn.close()
        logging.info(f"Worker{worker_id} disconnected")

# 결과 수신 및 장애 처리
def receive_results(conn, addr, worker_id):
    global result_matrix, task_queue
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            message = json.loads(data.decode())
            if message['type'] == 'result':
                i = message['i']
                j = message['j']
                success = message['success']
                if success:
                    result_matrix[i][j] = message['value']
                    logging.info(f"Result received from Worker{worker_id}: C[{i},{j}] = {message['value']}")
                else:
                    logging.info(f"Task failed at Worker{worker_id}: C[{i},{j}]")
                    task_queue.put((i, j))  # 재할당
    except Exception as e:
        logging.error(f"Error receiving results from Worker{worker_id}: {e}")
    finally:
        conn.close()

# 클라이언트 핸들러
def handle_worker(conn, addr, worker_id):
    assign_thread = threading.Thread(target=assign_tasks, args=(conn, addr, worker_id))
    receive_thread = threading.Thread(target=receive_results, args=(conn, addr, worker_id))
    assign_thread.start()
    receive_thread.start()

# 마스터 소켓 서버
def start_master_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_IP, MASTER_PORT))
    server.listen(len(WORKERS))
    logging.info(f"Master server started on {MASTER_IP}:{MASTER_PORT}")

    worker_id = 1
    while worker_id <= len(WORKERS):
        conn, addr = server.accept()
        handler_thread = threading.Thread(target=handle_worker, args=(conn, addr, worker_id))
        handler_thread.start()
        worker_id += 1

    server.close()

# 작업 생성
def generate_tasks():
    for i in range(MATRIX_SIZE):
        for j in range(MATRIX_SIZE):
            task_queue.put((i, j))

# 로그 기록 종료
def log_completion():
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"All tasks completed in {total_time:.2f} seconds")
    # 결과 행렬 저장
    np.savetxt("ResultMatrix.txt", result_matrix, fmt='%d')
    logging.info("Result matrix saved to ResultMatrix.txt")

if __name__ == "__main__":
    # 행렬 A와 B 생성
    A = np.random.randint(1, 100, size=(MATRIX_SIZE, MATRIX_SIZE))
    B = np.random.randint(1, 100, size=(MATRIX_SIZE, MATRIX_SIZE))
    logging.info("Matrices A and B generated")

    # 작업 생성
    generate_tasks()
    logging.info("All tasks generated and queued")

    # 마스터 서버 시작
    server_thread = threading.Thread(target=start_master_server)
    server_thread.start()

    # 모든 작업이 큐에서 소진될 때까지 대기
    task_queue.join()

    # 종료 신호 전송
    # Note: 여기서는 간단히 None을 넣어 종료하도록 했지만, 실제로는 각 워커에게 종료 신호를 보내야 합니다.
    # 이를 위해 추가적인 로직이 필요합니다.

    # 로그 기록
    log_completion()
