# worker.py
import socket
import threading
import json
import random
import time
import logging
import sys

# 설정 파일 로드
with open('config.json', 'r') as f:
    config = json.load(f)

MASTER_IP = config['master']['ip']
MASTER_PORT = config['master']['port']
WORKER_INFO = config['workers']

# 워커 ID 확인 (명령줄 인자로 전달)
if len(sys.argv) != 2:
    print("Usage: python worker.py <worker_id>")
    sys.exit(1)

worker_id = int(sys.argv[1])
if worker_id < 1 or worker_id > len(WORKER_INFO):
    print(f"Worker ID must be between 1 and {len(WORKER_INFO)}")
    sys.exit(1)

WORKER_IP = WORKER_INFO[worker_id - 1]['ip']
WORKER_PORT = WORKER_INFO[worker_id - 1]['port']

# 로그 설정
logging.basicConfig(filename=f'Worker{worker_id}.txt',
                    level=logging.INFO,
                    format='[%(asctime)s] %(message)s',
                    datefmt='%H:%M:%S')

# 최대 동시 작업 수
MAX_CONCURRENT_TASKS = 10
current_tasks = 0
tasks_lock = threading.Lock()

# 소켓 연결
def connect_to_master():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MASTER_IP, MASTER_PORT))
    logging.info(f"Connected to Master at {MASTER_IP}:{MASTER_PORT}")
    return s

# 작업 처리 함수
def handle_task(s, task):
    global current_tasks
    i = task['i']
    j = task['j']
    A_row = task['A_row']
    B_col = task['B_col']

    # 작업 수행 시간 (1~3초)
    work_time = random.uniform(1, 3)
    time.sleep(work_time)

    # 작업 성공 여부 (80% 성공)
    success = random.random() < 0.8

    if success:
        # C[i][j] 계산
        C_ij = sum(a * b for a, b in zip(A_row, B_col))
        # 결과 전송
        result = {
            'type': 'result',
            'i': i,
            'j': j,
            'value': C_ij,
            'success': True
        }
        s.sendall(json.dumps(result).encode())
        logging.info(f"Task completed: C[{i},{j}] = {C_ij}")
    else:
        # 실패 결과 전송
        result = {
            'type': 'result',
            'i': i,
            'j': j,
            'success': False
        }
        s.sendall(json.dumps(result).encode())
        logging.info(f"Task failed: C[{i},{j}]")

    with tasks_lock:
        current_tasks -= 1

# 워커의 작업 수 제한
def worker_listener(s):
    global current_tasks
    try:
        while True:
            data = s.recv(4096)
            if not data:
                break
            message = json.loads(data.decode())
            if message['type'] == 'task':
                with tasks_lock:
                    if current_tasks >= MAX_CONCURRENT_TASKS:
                        # 작업 과부하, 실패 처리
                        fail_result = {
                            'type': 'result',
                            'i': message['i'],
                            'j': message['j'],
                            'success': False
                        }
                        s.sendall(json.dumps(fail_result).encode())
                        logging.info(f"Task rejected due to overload: C[{message['i']},{message['j']}]")
                        continue
                    current_tasks += 1
                # 작업 스레드 시작
                task_thread = threading.Thread(target=handle_task, args=(s, message))
                task_thread.start()
    except Exception as e:
        logging.error(f"Error in listener: {e}")
    finally:
        s.close()
        logging.info("Disconnected from Master")

if __name__ == "__main__":
    while True:
        try:
            s = connect_to_master()
            worker_listener(s)
            break
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            time.sleep(5)  # 재시도 대기
