import socket
import threading
import numpy as np
import time
from queue import Queue
import json
import logging

# 로그 설정
logging.basicConfig(filename='master.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

class SystemClock:
    def __init__(self):
        """SystemClock 클래스는 시스템 시간을 기록하고 경과 시간을 계산하는 역할을 합니다."""
        self.start_time = time.time()

    def get_elapsed_time(self):
        """경과 시간을 반환합니다."""
        return time.time() - self.start_time

class MasterNode:
    def __init__(self, host='0.0.0.0', port=9999):
        """Master Node는 작업을 생성하고 Worker Node에 분배하는 역할을 담당합니다."""
        self.host = host
        self.port = port
        self.system_clock = SystemClock()  # 시스템 시계 생성
        self.worker_sockets = []  # 연결된 Worker 소켓 목록
        self.worker_ids = {}  # Worker ID 매핑
        self.worker_queues = {}  # 각 Worker의 작업 큐 상태
        self.A = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 A 생성
        self.B = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 B 생성
        self.C = np.zeros((1000, 1000))  # 결과 행렬 C 초기화
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐
        self.lock = threading.Lock()  # 스레드 간 자원 보호를 위한 잠금 장치

    def handle_worker(self, client_socket, address):
        """Worker가 연결될 때 ID를 할당하고 관리합니다."""
        worker_id = f"worker{len(self.worker_sockets) + 1}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        self.worker_queues[worker_id] = 0  # 초기 작업 큐 크기는 0으로 설정
        logging.info(f"{worker_id} connected from {address}")
        print(f"{worker_id} connected from {address}")
        
        if len(self.worker_sockets) == 4:  # Worker 4개가 모두 연결되면 작업 분배 시작
            logging.info("All 4 Worker Nodes are connected. Distributing tasks...")
            print("All 4 Worker Nodes are connected. Distributing tasks...")
            self.distribute_config()

    def add_tasks_to_queue(self):
        """행렬 곱셈 작업을 생성하여 작업 큐에 추가합니다."""
        for i in range(1000):
            for j in range(1000):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                
                with self.lock:
                    self.task_queue.put(task_data)
                    logging.info(f"Added task for C[{i}, {j}] to the task queue.")
                    print(f"Added task for C[{i}, {j}] to the task queue.")

    def send_data_with_size(self, socket, data):
        """데이터 크기를 먼저 보내고 데이터를 전송합니다."""
        data = data.encode('utf-8')
        data_size = len(data)
        socket.sendall(str(data_size).encode('utf-8'))  # 데이터 크기 전송
        ack = socket.recv(1024).decode()  # ACK 수신
        if ack == 'ACK':
            socket.sendall(data)  # 데이터 전송

    def receive_data_with_size(self, socket):
        """먼저 데이터 크기를 받고 데이터를 수신합니다."""
        data_size = int(socket.recv(1024).decode())  # 데이터 크기 수신
        socket.sendall('ACK'.encode('utf-8'))  # ACK 전송
        data = socket.recv(data_size).decode('utf-8')  # 지정된 크기만큼 데이터 수신
        return data

    def distribute_config(self):
        """Worker Node로 작업을 분배하는 스레드를 시작합니다."""
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()

        task_thread = threading.Thread(target=self.distribute_tasks)
        task_thread.start()

    def distribute_tasks(self):
        """Worker Node에 작업을 분배합니다."""
        while True:
            if not self.failed_queue.empty():
                # 실패한 작업을 우선적으로 분배
                with self.lock:
                    failed_task = self.failed_queue.get()
                for worker_socket in self.worker_sockets:
                    worker_id = self.worker_ids[worker_socket]
                    if self.worker_queues[worker_id] < 10:  # 큐가 가득 차지 않은 Worker에만 분배
                        self.send_data_with_size(worker_socket, failed_task)
                        logging.info(f"Resent failed task to {worker_id}")
                        print(f"Resent failed task to {worker_id}")
                        self.worker_queues[worker_id] += 1
                        break
            elif not self.task_queue.empty():
                # 일반 작업을 분배
                with self.lock:
                    task_data = self.task_queue.get()
                for worker_socket in self.worker_sockets:
                    worker_id = self.worker_ids[worker_socket]
                    if self.worker_queues[worker_id] < 10:  # 큐가 가득 차지 않은 Worker에만 분배
                        self.send_data_with_size(worker_socket, task_data)
                        logging.info(f"Sent task to {worker_id}")
                        print(f"Sent task to {worker_id}")
                        self.worker_queues[worker_id] += 1
                        break
            time.sleep(1)

    def receive_results(self, worker_socket):
        """Worker Node로부터 결과를 수신하고, 성공 여부에 따라 작업을 처리합니다."""
        try:
            while True:
                result = self.receive_data_with_size(worker_socket)
                worker_id = self.worker_ids[worker_socket]
                
                if "failed" in result:
                    # 실패한 작업을 다시 큐에 넣음
                    logging.info(f"Received failure from {worker_id}: {result}")
                    print(f"Received failure from {worker_id}: {result}")
                    task_data = result.split("failed task for ")[1]
                    with self.lock:
                        self.failed_queue.put(task_data)
                    self.worker_queues[worker_id] -= 1
                else:
                    # 작업이 성공하면 결과 행렬을 업데이트
                    logging.info(f"Received result from {worker_id}: {result}")
                    print(f"Received result from {worker_id}: {result}")
                    result_data = json.loads(result)
                    i, j, value = result_data['i'], result_data['j'], result_data['value']
                    with self.lock:
                        self.C[i][j] = value
                    self.worker_queues[worker_id] -= 1

                # 모든 작업이 완료되었는지 확인
                if self.task_queue.empty() and self.failed_queue.empty():
                    logging.info(f"All tasks completed in {self.system_clock.get_elapsed_time()} seconds.")
                    print(f"All tasks completed in {self.system_clock.get_elapsed_time()} seconds.")
                    logging.info("Final matrix C:\n" + str(self.C))
                    print("Final matrix C:\n", self.C)
                    break
        except Exception as e:
            logging.error(f"Error receiving result from {worker_id}: {e}")
            print(f"Error receiving result from {worker_id}: {e}")

    def run(self):
        """Master Node 실행을 위한 메인 함수입니다."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        logging.info(f"Master Node started on {self.host}:{self.port}")
        print(f"Master Node started on {self.host}:{self.port}")

        while len(self.worker_sockets) < 4:
            client_socket, address = server_socket.accept()
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))
            worker_thread.start()

        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)
    master_node.run()
