import socket
import random
import time
import json
from queue import Queue, Full
import threading
import logging

# 로그 설정
logging.basicConfig(filename='worker.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

class SystemClock:
    def __init__(self):
        """SystemClock 클래스는 시스템 시간을 기록하고 경과 시간을 계산하는 역할을 합니다."""
        self.start_time = time.time()

    def get_elapsed_time(self):
        """경과 시간을 반환합니다."""
        return time.time() - self.start_time

class WorkerNode:
    def __init__(self, master_host, master_port):
        """Worker Node는 Master Node로부터 작업을 받아 처리합니다."""
        self.master_host = master_host
        self.master_port = master_port
        self.system_clock = SystemClock()
        self.task_queue = Queue(maxsize=10)  # 최대 10개의 작업을 처리할 수 있는 큐
        self.success_count = 0  # 성공한 작업 수
        self.failure_count = 0  # 실패한 작업 수
        self.neighbor_socket = None  # 이웃 Worker와의 통신 소켓

    def connect_to_master(self):
        """Master Node에 연결하고 Worker ID를 할당받습니다."""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        logging.info(f"Connected to Master Node at {self.master_host}:{self.master_port}")
        print(f"Connected to Master Node at {self.master_host}:{self.master_port}")

        self.worker_id = self.client_socket.recv(1024).decode()
        logging.info(f"Assigned Worker ID: {self.worker_id}")
        print(f"Assigned Worker ID: {self.worker_id}")

    def receive_data_with_size(self, socket):
        """먼저 데이터 크기를 받고 데이터를 수신합니다."""
        data_size = int(socket.recv(1024).decode())  # 데이터 크기 수신
        socket.sendall('ACK'.encode('utf-8'))  # ACK 전송
        data = socket.recv(data_size).decode('utf-8')  # 지정된 크기만큼 데이터 수신
        return data

    def send_data_with_size(self, socket, data):
        """데이터 크기를 먼저 보내고 데이터를 전송합니다."""
        data = data.encode('utf-8')
        data_size = len(data)
        socket.sendall(str(data_size).encode('utf-8'))  # 데이터 크기 전송
        ack = socket.recv(1024).decode()  # ACK 수신
        if ack == 'ACK':
            socket.sendall(data)  # 데이터 전송

    def receive_task(self):
        """Master Node로부터 작업을 수신하여 작업 큐에 저장합니다."""
        while True:
            try:
                task_data = self.receive_data_with_size(self.client_socket)
                if task_data:
                    try:
                        self.task_queue.put(task_data, timeout=1)
                        logging.info(f"{self.worker_id} received task: {task_data}")
                        print(f"{self.worker_id} received task")
                    except Full:
                        # 작업 큐가 가득 찬 경우
                        logging.info(f"{self.worker_id}'s queue is full. Trying to redistribute the task.")
                        print(f"{self.worker_id}'s queue is full. Trying to redistribute the task.")
                        self.redistribute_task(task_data)
            except Exception as e:
                logging.error(f"Error receiving task: {e}")
                print(f"Error receiving task: {e}")
                break

    def redistribute_task(self, task_data):
        """작업을 다른 Worker Node로 재분배합니다."""
        if not self.neighbor_socket:
            self.connect_to_neighbor()

        try:
            self.send_data_with_size(self.neighbor_socket, task_data)
            logging.info(f"{self.worker_id} redistributed task to neighbor.")
            print(f"{self.worker_id} redistributed task to neighbor.")
        except Exception as e:
            logging.error(f"{self.worker_id} failed to redistribute task to neighbor: {e}")
            print(f"{self.worker_id} failed to redistribute task to neighbor: {e}")
            # 재분배가 실패하면 Master에 알림
            self.send_data_with_size(self.client_socket, f"{self.worker_id} failed to redistribute task")

    def connect_to_neighbor(self):
        """이웃 Worker Node에 연결합니다. (부하 분산을 위해 사용됨)"""
        self.neighbor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 이웃 Worker Node의 IP와 포트를 설정해야 함
        neighbor_host = "127.0.0.1"
        neighbor_port = 8888
        self.neighbor_socket.connect((neighbor_host, neighbor_port))
        logging.info(f"{self.worker_id} connected to neighbor Worker Node.")
        print(f"{self.worker_id} connected to neighbor Worker Node.")

    def process_task(self):
        """작업 큐에서 작업을 꺼내어 처리합니다."""
        while True:
            if not self.task_queue.empty():
                task_data = self.task_queue.get()

                try:
                    task = json.loads(task_data)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to decode task data: {e}")
                    print(f"Failed to decode task data: {e}")
                    continue

                i, j, A_row, B_col = task['i'], task['j'], task['A_row'], task['B_col']
                logging.info(f"{self.worker_id} is processing task for C[{i}, {j}]")
                print(f"{self.worker_id} is processing task for C[{i}, {j}]")

                try:
                    time.sleep(random.uniform(1, 3))

                    if random.random() < 0.8:
                        value = sum(a * b for a, b in zip(A_row, B_col))
                        result = json.dumps({'i': i, 'j': j, 'value': value})
                        self.send_data_with_size(self.client_socket, result)
                        logging.info(f"{self.worker_id} completed task for C[{i}, {j}]")
                        print(f"{self.worker_id} completed task for C[{i}, {j}]")
                        self.success_count += 1
                    else:
                        raise Exception("Random failure occurred")
                except Exception as e:
                    logging.error(f"{self.worker_id} failed to process task: {e}")
                    print(f"{self.worker_id} failed to process task: {e}")
                    self.failure_count += 1
                    self.send_data_with_size(self.client_socket, f"{self.worker_id} failed task for C[{i}, {j}]")

    def run(self):
        """Worker Node 실행을 위한 메인 함수입니다."""
        self.connect_to_master()

        # Master Node로부터 작업을 수신하고 처리
        threading.Thread(target=self.receive_task).start()
        threading.Thread(target=self.process_task).start()

if __name__ == "__main__":
    worker_node = WorkerNode(master_host="127.0.0.1", master_port=9999)
    worker_node.run()
