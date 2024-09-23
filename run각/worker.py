import socket
import random
import time
import json
from queue import Queue, Full
import threading

class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

class WorkerNode:
    def __init__(self, master_host, master_port):
        self.worker_id = None  # Master Node에서 할당받은 Worker ID
        self.master_host = master_host
        self.master_port = master_port
        self.task_queue = Queue(maxsize=1)  # 작업 큐, 한 번에 하나의 작업만 처리
        self.success_count = 0
        self.failure_count = 0

    def connect_to_master(self):
        # Master Node에 연결
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Master Node와 연결 {self.master_host}:{self.master_port}")

        # Worker ID 할당받기 (Master Node에서 할당)
        self.worker_id = self.client_socket.recv(1024).decode()
        print(f"Worker ID 할당: {self.worker_id}")

    def request_task(self):
        # Master Node에 작업 요청
        self.client_socket.sendall("REQUEST_TASK".encode('utf-8'))
        print(f"{self.worker_id} 작업 요청")

    def receive_task(self):
        buffer = ""  # 데이터 버퍼
        while True:
            try:
                task_data = self.client_socket.recv(1024).decode()  # 작업 수신
                if task_data:
                    buffer += task_data  # 버퍼에 데이터 추가
                    if "<END>" in buffer:  # 구분자 확인
                        complete_task = buffer.split("<END>")[0]  # 구분자를 기준으로 작업 분리
                        buffer = buffer.split("<END>")[1]  # 나머지 데이터는 버퍼에 저장
                        
                        # 큐가 가득 찼는지 확인
                        try:
                            self.task_queue.put(complete_task, timeout=1)
                            print(f"수신: {self.worker_id}")
                            self.process_task()
                        except Full:
                            print(f"{self.worker_id} 큐가 가득 참")
                            self.client_socket.sendall(f"{self.worker_id} failed to receive task".encode('utf-8'))
            except Exception as e:
                print(f"Error receiving task: {e}")
                break

    def process_task(self):
        if not self.task_queue.empty():
            task_data = self.task_queue.get()  # 작업 큐에서 작업 꺼내기

            # json으로 전달된 데이터를 역직렬화하여 처리
            task = json.loads(task_data)
            i, j, A_row, B_col = task['i'], task['j'], task['A_row'], task['B_col']

            print(f"처리: {self.worker_id} / C[{i}, {j}]")

            try:
                # 작업 처리 (랜덤하게 1~3초 소요되는 작업 시뮬레이션)
                time.sleep(random.uniform(1, 3))

                # 연산 성공/실패 확률 적용 (80% 성공, 20% 실패)
                if random.random() < 0.8:
                    # 성공 시: 성공 메시지 전송
                    self.client_socket.sendall(f"{self.worker_id} 성공: C[{i}, {j}]".encode('utf-8'))
                    self.success_count += 1
                else:
                    raise Exception("Random failure occurred")
            except Exception as e:
                # 작업 실패 시 Master Node에 실패 메시지 전송
                print(f"{self.worker_id} 실패: C[{i}, {j}], {e}")
                self.client_socket.sendall(f"{self.worker_id} failed task for C[{i}, {j}]".encode('utf-8'))
                self.failure_count += 1

            # 작업 완료 후 새로운 작업 요청
            self.request_task()

    def run(self):
        # Master Node와 연결
        self.connect_to_master()

        # 첫 번째 작업 요청
        self.request_task()

        # 작업 수신 대기 스레드 실행
        threading.Thread(target=self.receive_task).start()

# Worker Node 실행
if __name__ == "__main__":
    worker_node = WorkerNode(master_host="127.0.0.1", master_port=9999)
    worker_node.run()
