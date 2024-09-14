import socket
import threading
import random
import numpy as np
import time

class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

class WorkerNode:
    def __init__(self, worker_id, master_host, master_port):
        self.worker_id = worker_id
        self.master_host = master_host
        self.master_port = master_port
        self.system_clock = SystemClock()
        self.task_queue = []
        self.success_count = 0
        self.failure_count = 0

    def connect_to_master(self):
        # Master Node에 연결
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Worker {self.worker_id} connected to Master Node at {self.master_host}:{self.master_port}")

    def receive_task(self):
        # Master Node로부터 작업 수신 (데이터 예시: i, j, A_row, B_col)
        task_data = self.client_socket.recv(1024).decode()
        if task_data:
            i, j, A_row, B_col = eval(task_data)  # 간단한 데이터 구조 해석
            self.task_queue.append((i, j, A_row, B_col))

    def process_task(self):
        while True:
            if len(self.task_queue) > 0:
                i, j, A_row, B_col = self.task_queue.pop(0)
                try:
                    # 작업 수행 (80% 성공, 20% 실패)
                    if random.random() < 0.8:
                        result = sum(a * b for a, b in zip(A_row, B_col))
                        self.success_count += 1
                        self.client_socket.sendall(f"Success: C[{i}, {j}] = {result}".encode())
                    else:
                        raise Exception("Task failed")
                except Exception as e:
                    self.failure_count += 1
                    self.client_socket.sendall(f"Failure: C[{i}, {j}]".encode())
                time.sleep(random.uniform(1, 3))  # 작업 시간 1~3초

    def log_event(self, message):
        with open(f'Worker{self.worker_id}.txt', 'a') as log_file:
            log_file.write(f"{self.system_clock.get_elapsed_time():.2f} - {message}\n")

    def run(self):
        self.connect_to_master()
        while True:
            self.receive_task()
            self.process_task()

# 외부 통신을 위한 Worker Node
if __name__ == "__main__":
    worker_node = WorkerNode(worker_id=1, master_host="<VM_EXTERNAL_IP>", master_port=9999)  # Google Cloud VM 외부 IP 주소
    worker_node.run()
