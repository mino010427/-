import socket
import threading
import numpy as np
import time
from queue import Queue
import json

# 가상의 System Clock 정의
class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

# Master Node 클래스 정의
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
        self.C = np.zeros((1000, 1000))  # 결과 행렬 C
        self.task_queue = Queue()  # 작업 재할당 큐

    def handle_worker(self, client_socket, address):
        # Worker Node를 연결 목록에 추가 및 ID 할당
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        print(f"{worker_id} connected from {address}")

        # 모든 Worker Node가 연결될 때까지 대기
        if self.connected_workers == 4:
            print("All 4 Worker Nodes are connected. Distributing tasks...")
            self.distribute_tasks()

    def distribute_tasks(self):
        # 1000x1000 행렬의 연산 작업을 각 Worker Node에게 분배
        num_tasks = 1000 // len(self.worker_sockets)  # 각 Worker Node에 할당할 작업 수

        for worker_index, worker_socket in enumerate(self.worker_sockets):
            for i in range(worker_index * num_tasks, (worker_index + 1) * num_tasks):
                for j in range(1000):
                    A_row = self.A[i, :]
                    B_col = self.B[:, j]
                    # JSON 형식으로 데이터를 변환
                    task_data = json.dumps({
                        "i": i,
                        "j": j,
                        "A_row": A_row.tolist(),
                        "B_col": B_col.tolist()
                    })
                    worker_socket.send(task_data.encode('utf-8'))  # 작업을 Worker Node에게 전송
                    print(f"Sent task for C[{i}, {j}] to {self.worker_ids[worker_socket]}")

        # Worker Node로부터 작업 결과 수신
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()

    def receive_results(self, worker_socket):
        # 각 Worker Node로부터 결과 수신 및 재할당 처리
        try:
            while True:
                result = worker_socket.recv(4096).decode()  # 버퍼 크기를 늘림
                if result:
                    if "failed" in result:
                        # 작업 실패 시 재할당
                        print(f"[{self.system_clock.get_elapsed_time():.2f}] Failure from {self.worker_ids[worker_socket]}: {result}")
                        task_data = result.split("failed task for ")[1]
                        self.task_queue.put(task_data)  # 실패한 작업을 큐에 추가
                        self.reassign_failed_task()
                    else:
                        print(f"[{self.system_clock.get_elapsed_time():.2f}] Result from {self.worker_ids[worker_socket]}: {result}")
                time.sleep(1)  # 통신 지연 시뮬레이션
        except Exception as e:
            print(f"Error receiving result from {self.worker_ids[worker_socket]}: {e}")

    def reassign_failed_task(self):
        # 작업 재할당
        retry_limit = 3  # 재시도 제한
        while not self.task_queue.empty():
            task_data = self.task_queue.get()
            retry_count = 0
            success = False
            while retry_count < retry_limit and not success:
                for worker_socket in self.worker_sockets:
                    try:
                        worker_socket.send(task_data.encode('utf-8'))
                        print(f"Reassigned task: {task_data} to {self.worker_ids[worker_socket]}")
                        success = True
                        break
                    except Exception as e:
                        print(f"Error reassigning task: {e}")
                        retry_count += 1
            if not success:
                print(f"Task {task_data} failed after {retry_limit} attempts")

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

        print("All 4 Worker Nodes are connected. Proceeding with task distribution...")

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()
