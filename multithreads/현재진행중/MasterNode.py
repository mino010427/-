import socket
import threading
import numpy as np
import time
from queue import Queue
import json

class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

class MasterNode:
    def __init__(self, host='0.0.0.0', port=9999):
        self.host = host
        self.port = port
        self.system_clock = SystemClock()
        self.worker_sockets = []
        self.connected_workers = 0  # 접속한 Worker Node 수
        self.worker_ids = {}  # Worker ID 매핑
        self.A = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 A
        self.B = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 B
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐 추가
        self.lock = threading.Lock()  # 뮤텍스 추가

    def handle_worker(self, client_socket, address):
        # Worker Node를 연결 목록에 추가 및 ID 할당
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        print(f"{worker_id} connected from {address}")

        # 모든 Worker Node가 연결될 때까지 대기
        while True:
            if self.connected_workers == 4:
                print("All 4 Worker Nodes are connected. Distributing tasks...")
                self.distribute_config()
                break

    def add_tasks_to_queue(self):
        # 모든 작업을 작업 큐에 추가
        for i in range(1000):
            for j in range(1000):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                
                with self.lock:  # 큐에 접근할 때 뮤텍스 잠금
                    self.task_queue.put(task_data)

    def distribute_config(self):
        # 각 Worker Node가 작업을 수신할 수 있도록 스레드를 시작함
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()

        # 작업 분배를 위한 스레드 시작
        distribution_thread = threading.Thread(target=self.distribute_tasks)
        distribution_thread.start()

    def distribute_tasks(self):
        while True:
            # 먼저 실패한 작업이 있는지 확인
            if not self.failed_queue.empty():
                with self.lock:
                    failed_task_data = self.failed_queue.get()
                # 실패한 작업을 우선적으로 분배
                for worker_socket in self.worker_sockets:
                    worker_socket.send(failed_task_data.encode('utf-8'))
                    print(f"Resent failed task to {self.worker_ids[worker_socket]}")
                    break  # 한 번에 하나의 실패 작업만 분배 후 다시 대기
            else: # Worker Node에 동적으로 작업을 분배
                # 실패한 작업이 없으면 일반 작업 큐에서 작업 분배
                if not self.task_queue.empty():
                    with self.lock: # 작업 큐에서 꺼낼 때 뮤텍스 잠금
                        task_data = self.task_queue.get()
                    for worker_socket in self.worker_sockets:
                        worker_socket.send(task_data.encode('utf-8'))
                        print(f"Sent task to {self.worker_ids[worker_socket]}")
            time.sleep(1) # 분배 주기 조절

    def receive_results(self, worker_socket):
        # 각 Worker Node로부터 결과 수신 및 재할당 처리
        try:
            while True:
                result = worker_socket.recv(1024).decode()
                if result:
                    if "failed" in result:
                        # 작업 실패 시 재할당
                        print(f"Received failure from {self.worker_ids[worker_socket]}: {result}")
                        task_data = result.split("failed task for ")[1]
                        
                        with self.lock:  # 작업 큐에 실패한 작업을 추가할 때 뮤텍스 잠금
                            self.failed_queue.put(task_data)  # 실패한 작업을 실패 큐에 추가
                    else:
                        print(f"Received result from {self.worker_ids[worker_socket]}: {result}")
                time.sleep(1)  # 통신 지연 시뮬레이션
        except Exception as e:
            print(f"Error receiving result from {self.worker_ids[worker_socket]}: {e}")

    def run(self):
        # 소켓 설정
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"Master Node started on {self.host}:{self.port}")

        # Worker Node의 접속을 기다림
        while self.connected_workers < 4:
            client_socket, address = server_socket.accept()
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))
            worker_thread.start()

        # 작업 추가를 위한 스레드 시작
        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

        print("All 4 Worker Nodes are connected. Proceeding with task distribution...")

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()
