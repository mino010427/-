import socket
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

    def connect_to_master(self):
        # Master Node에 연결
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Worker {self.worker_id} connected to Master Node at {self.master_host}:{self.master_port}")

    def receive_task_and_process(self):
        # Master Node로부터 작업 수신 및 처리
        while True:
            task_data = self.client_socket.recv(1024).decode()
            if task_data:
                i, j, A_row, B_col = eval(task_data)
                print(f"Worker {self.worker_id} received task for C[{i}, {j}]")
                
                # 작업 처리 (랜덤하게 1~3초 소요되는 작업 시뮬레이션)
                time.sleep(random.uniform(1, 3))

                # 행렬 곱 연산 수행
                result = sum(a * b for a, b in zip(A_row, B_col))

                # 처리된 결과를 Master Node에 전송
                self.client_socket.sendall(f"Worker {self.worker_id} result: C[{i}, {j}] = {result}".encode('utf-8'))

    def run(self):
        self.connect_to_master()
        self.receive_task_and_process()

# 외부 통신을 위한 Worker Node
if __name__ == "__main__":
    worker_id = int(input("Enter Worker ID: "))  # Worker ID를 입력받음
    worker_node = WorkerNode(worker_id=worker_id, master_host="34.68.170.234", master_port=9999)  # Google Cloud VM 외부 IP 주소
    worker_node.run()
