import socket
import threading
import random
import time
import sys
import logging

# 설정
HOST = '34.68.170.234'  # Master node의 IP 주소
PORT = 5000          # Master node의 포트 번호
MAX_TASKS = 10       # 최대 작업 수
WORKER_ID = 1        # 워커의 ID, 여러 워커에서 각각 다르게 설정해야 함

# 로깅 설정
logging.basicConfig(filename=f'Worker{WORKER_ID}.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

def simulate_task():
    """ 작업 시뮬레이션: 1~3초 동안 랜덤하게 지연됨 """
    time.sleep(random.uniform(1, 3))

def handle_task(task_data, conn):
    """ 작업 처리 함수 """
    try:
        # 작업 처리 시뮬레이션
        success = random.random() < 0.8  # 80% 확률로 성공
        if success:
            simulate_task()
            result = task_data  # 결과는 단순히 입력 데이터를 반환
            conn.sendall(f'SUCCESS {result}'.encode())
            logging.info(f"Task completed successfully: {result}")
        else:
            conn.sendall('FAILURE'.encode())
            logging.info("Task failed")
    except Exception as e:
        logging.error(f"Error during task processing: {e}")

def worker_thread(conn):
    """ 워커 스레드 함수 """
    try:
        logging.info("Worker thread started")
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            
            if data.startswith('TASK'):
                task_data = data[5:]  # TASK 명령어 뒤의 데이터 추출
                handle_task(task_data, conn)
            else:
                logging.warning(f"Unknown command received: {data}")
    except Exception as e:
        logging.error(f"Error in worker thread: {e}")
    finally:
        conn.close()
        logging.info("Worker thread ended")

def main():
    """ 메인 함수 """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            logging.info("Connected to master node")
            
            while True:
                task_data = s.recv(1024).decode()
                if not task_data:
                    break
                
                if task_data.startswith('TASK'):
                    threading.Thread(target=worker_thread, args=(s,)).start()
                else:
                    logging.warning(f"Received unknown data: {task_data}")
    except KeyboardInterrupt:
        logging.info("Worker interrupted and shutting down")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        s.close()

if __name__ == "__main__":
    main()
