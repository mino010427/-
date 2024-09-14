import socket
import threading
import numpy as np
from queue import Queue
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
        self.task_queue = Queue()
        self.worker_sockets = []
        self.C = np.zeros((1000, 1000))  # 결과 행렬
        self.connected_workers = 0  # 접속한 Worker Node 수

    def handle_worker(self, client_socket, address):
        try:
            # Worker Node에 작업 전송 (i, j, A_row, B_col)
            while not self.task_queue.empty():
                i, j, A_row, B_col = self.task_queue.get()
                task_data = str((i, j, list(A_row), list(B_col)))  # 데이터를 문자열로 변환
                client_socket.send(task_data.encode('utf-8'))
                print(f"[{self.system_clock.get_elapsed_time():.2f} sec] Sent task {i},{j} to Worker {address}")

                # Worker Node로부터 연산 결과 수신
                data = client_socket.recv(1024).decode()
                if data:
                    print(f"[{self.system_clock.get_elapsed_time():.2f} sec] Received result from {address}: {data}")
                time.sleep(1)  # 통신 딜레이 시뮬레이션

            # 소켓 닫기
            client_socket.close()
        except Exception as e:
            print(f"Error handling worker {address}: {e}")
            client_socket.close()

    def distribute_tasks(self):
        # 1000x1000 행렬 생성
        A = np.random.randint(1, 100, (1000, 1000))
        B = np.random.randint(1, 100, (1000, 1000))

        # 각 작업을 큐에 추가 (C[i, j] 연산 작업)
        for i in range(1000):
            for j in range(1000):
                self.task_queue.put((i, j, A[i], B[:, j]))  # 각 열에 대한 작업 추가

    def log_event(self, message):
        with open('Master.txt', 'a') as log_file:
            log_file.write(f"{self.system_clock.get_elapsed_time():.2f} - {message}\n")

    def run(self):
        # 소켓 설정
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"Master Node started on {self.host}:{self.port}")

        self.distribute_tasks()

        while self.connected_workers < 4:  # 4개의 Worker Node가 접속할 때까지 대기
            # Worker Node와 연결 수립
            client_socket, address = server_socket.accept()
            self.connected_workers += 1
            print(f"Connected to Worker Node at {address} (Total connected: {self.connected_workers})")
            self.worker_sockets.append(client_socket)

            # Worker Node와의 통신을 처리할 스레드 생성
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))
            worker_thread.start()

        print("All 4 Worker Nodes are connected. Proceeding with task distribution...")

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()
