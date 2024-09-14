import socket
import threading
import numpy as np
from queue import Queue
import time

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
        self.task_queue = Queue()
        self.worker_sockets = []
        self.C = np.zeros((1000, 1000))  # 결과 행렬

    def handle_worker(self, client_socket, address):
        while True:
            try:
                # Worker Node로부터 데이터 수신 (작업 결과 혹은 실패 메시지)
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                print(f"[{self.system_clock.get_elapsed_time():.2f} sec] Received from {address}: {data}")

                # 작업 재할당 혹은 추가 처리에 대한 로직 구현 필요
                # 예시: 다른 Worker Node로 작업 재분배 등
                # self.reassign_task()
            except Exception as e:
                print(f"Error handling worker {address}: {e}")
                break

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

        while True:
            # Worker Node와 연결 수립
            client_socket, address = server_socket.accept()
            print(f"Connected to Worker Node at {address}")
            self.worker_sockets.append(client_socket)

            # Worker Node와의 통신을 처리할 스레드 생성
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))
            worker_thread.start()

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()
