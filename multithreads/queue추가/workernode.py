import socket
import random
import json
import time
from queue import Queue, Full, Empty
import threading

class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

class WorkerNode:
    def __init__(self, master_host, master_port, worker_port, worker_peers):
        self.worker_id = None  # Master Node에서 할당받은 Worker ID
        self.master_host = master_host
        self.master_port = master_port
        self.worker_port = worker_port
        self.worker_peers = worker_peers  # 다른 Worker Node의 주소 [(IP, port), ...]
        self.system_clock = SystemClock()
        self.task_queue = Queue(maxsize=10)  # 작업 큐, 최대 10개의 작업만 허용
        self.success_count = 0
        self.failure_count = 0
        self.worker_sockets = {}  # 다른 Worker Node와의 연결 소켓

    def connect_to_master(self):
        # Master Node에 연결
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Connected to Master Node at {self.master_host}:{self.master_port}")

        # Worker ID 할당받기 (Master Node에서 할당)
        self.worker_id = self.client_socket.recv(1024).decode()
        print(f"Assigned Worker ID: {self.worker_id}")

    def communicate_with_other_workers(self):
        # Worker Node 간 통신을 위한 소켓 서버 설정
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("0.0.0.0", self.worker_port))
        server_socket.listen(5)
        print(f"{self.worker_id} is ready to communicate with other Workers on port {self.worker_port}")

        # 다른 Worker Node들과의 연결
        for peer in self.worker_peers:
            worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                worker_socket.connect(peer)
                self.worker_sockets[peer] = worker_socket
                print(f"{self.worker_id} connected to Worker at {peer}")
            except Exception as e:
                print(f"Error connecting to {peer}: {e}")

        # 수신 대기 스레드
        while True:
            worker_socket, address = server_socket.accept()
            print(f"Connected to another Worker from {address}")
            threading.Thread(target=self.handle_worker_communication, args=(worker_socket,)).start()

    def handle_worker_communication(self, worker_socket):
        # 다른 Worker Node와의 통신
        while True:
            try:
                data = worker_socket.recv(1024).decode()
                if data:
                    print(f"{self.worker_id} received task from another Worker: {data}")
                    self.task_queue.put(data)  # 받은 작업을 자신의 큐에 추가
            except Exception as e:
                print(f"Error communicating with other Worker: {e}")
                break

    def send_task_to_other_worker(self, task_data):
        # 부하가 걸릴 때 다른 Worker Node에게 작업 전송
        for peer_socket in self.worker_sockets.values():
            try:
                peer_socket.send(task_data.encode('utf-8'))
                print(f"{self.worker_id} sent task to another Worker")
                return True  # 작업을 성공적으로 넘김
            except Exception as e:
                print(f"Error sending task to another Worker: {e}")
        return False  # 작업을 넘기지 못함

    def receive_task(self):
        # Master Node로부터 작업 수신
        while True:
            try:
                task_data = self.client_socket.recv(4096).decode()  # 버퍼 크기를 늘림
                if task_data:
                    # 큐가 가득 찬 경우 다른 Worker Node에게 작업을 넘김
                    try:
                        self.task_queue.put(task_data, timeout=1)  # 큐에 작업을 추가
                        print(f"{self.worker_id} received task")
                    except Full:
                        print(f"{self.worker_id}'s queue is full. Trying to pass task to another Worker.")
                        if not self.send_task_to_other_worker(task_data):
                            print(f"{self.worker_id} failed to pass task. Task failed.")
                            self.failure_count += 1
                            self.client_socket.sendall(f"{self.worker_id} failed task for {task_data}".encode())
            except Exception as e:
                print(f"Error receiving task: {e}")
                break

    def process_task(self):
        # 작업 처리
        while True:
            if not self.task_queue.empty():
                task_data = self.task_queue.get()  # 작업 큐에서 작업 꺼내기
                task_info = json.loads(task_data)  # JSON으로 변환된 데이터를 파싱
                i = task_info["i"]
                j = task_info["j"]
                A_row = task_info["A_row"]
                B_col = task_info["B_col"]

                print(f"{self.worker_id} is processing task for C[{i}, {j}]")

                try:
                    # 작업 처리 (랜덤하게 1~3초 소요되는 작업 시뮬레이션)
                    time.sleep(random.uniform(1, 3))

                    # 연산 성공/실패 확률 적용 (80% 성공, 20% 실패)
                    if random.random() < 0.8:
                        result = sum(a * b for a, b in zip(A_row, B_col))
                        self.client_socket.sendall(f"{self.worker_id} result: C[{i}, {j}] = {result}".encode('utf-8'))
                        self.success_count += 1
                    else:
                        raise Exception("Random failure occurred")
                except Exception as e:
                    # 작업 실패 시 Master Node에 실패 메시지 전송
                    print(f"{self.worker_id} failed to process task: {e}")
                    self.failure_count += 1
                    self.client_socket.sendall(f"{self.worker_id} failed task for C[{i}, {j}]".encode('utf-8'))

    def run(self):
        # Master Node와 연결
        self.connect_to_master()

        # Worker Node 간 통신 스레드를 실행
        threading.Thread(target=self.communicate_with_other_workers).start()

        # 작업 수신 및 처리 스레드를 생성
        threading.Thread(target=self.receive_task).start()  # 작업 수신 스레드
        threading.Thread(target=self.process_task).start()  # 작업 처리 스레드

# 외부 통신을 위한 Worker Node
if __name__ == "__main__":
    worker_port = int(input("Enter Worker Communication Port: "))  # Worker 간 통신 포트 입력
    peers = input("Enter peer worker addresses (format: IP:port,IP:port,...): ").split(',')
    peer_addresses = [tuple(peer.split(':')) for peer in peers]
    peer_addresses = [(ip, int(port)) for ip, port in peer_addresses]

    worker_node = WorkerNode(master_host="34.68.170.234", master_port=9999, worker_port=worker_port, worker_peers=peer_addresses)
    worker_node.run()
