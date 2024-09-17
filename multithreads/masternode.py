import socket
import threading
import numpy as np
import time

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
        self.A = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 A
        self.B = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 B
        self.C = np.zeros((1000, 1000))  # 결과 행렬 C

    def handle_worker(self, client_socket, address):
        # Worker Node를 연결 목록에 추가
        self.worker_sockets.append(client_socket)
        self.connected_workers += 1
        print(f"Worker {address} connected (Total connected: {self.connected_workers})")

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
                    task_data = str((i, j, A_row.tolist(), B_col.tolist()))  # 데이터를 문자열로 변환
                    worker_socket.send(task_data.encode('utf-8'))
                    print(f"Sent task for C[{i}, {j}] to Worker {worker_index+1}")

        # Worker Node로부터 작업 결과 수신
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()

    def receive_results(self, worker_socket):
        # 각 Worker Node로부터 결과 수신
        try:
            while True:
                result = worker_socket.recv(1024).decode()
                if result:
                    print(f"Received result from Worker: {result}")
                time.sleep(1)  # 통신 지연 시뮬레이션
        except Exception as e:
            print(f"Error receiving result: {e}")

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
