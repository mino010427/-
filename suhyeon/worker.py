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
        self.system_clock = SystemClock()
        self.task_queue = Queue(maxsize=10)  # 작업 큐, 최대 10개의 작업만 허용
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

    def receive_task(self):
        buffer = ""  # 데이터 버퍼
        print('Master Node로부터 작업 수신 시작')
        while True:
            try:
                task_data = self.client_socket.recv(1024).decode()  # 데이터를 수신
                if task_data == "QUEUE_STATUS":  # Master에서 작업 큐 상태를 요청하면
                    queue_size = self.task_queue.qsize()  # 현재 큐에 있는 작업 수 반환
                    self.client_socket.send(str(queue_size).encode('utf-8'))
                else:
                    buffer += task_data  # 일반 작업 수신 시 처리
                    if "<END>" in buffer:
                        complete_task = buffer.split("<END>")[0]
                        buffer = buffer.split("<END>")[1]

                        try:
                            self.task_queue.put(complete_task, timeout=1)
                            print(f"작업수신: {self.worker_id}")
                        except Full:
                            print(f"작업실패: {self.worker_id}의 queue가 가득참")
                            self.failure_count += 1
                            self.client_socket.sendall(f"{self.worker_id} failed to receive task".encode())
            except Exception as e:
                print(f"Error receiving task: {e}")
                break


    def process_task(self):
        # 작업 처리
        while True:
            if not self.task_queue.empty():
                task_data = self.task_queue.get()  # 작업 큐에서 작업 꺼내기

                # json으로 전달된 데이터를 역직렬화하여 처리
                task = json.loads(task_data)
                i, j, A_row, B_col = task['i'], task['j'], task['A_row'], task['B_col']

                print(f"작업처리: {self.worker_id} / C[{i}, {j}]")

                try:
                    # 작업 처리 (랜덤하게 1~3초 소요되는 작업 시뮬레이션)
                    time.sleep(random.uniform(1, 3))

                    # 연산 성공/실패 확률 적용 (80% 성공, 20% 실패)
                    if random.random() < 0.8:
                        # 성공 시: 행렬 결과를 보내지 않고, 인덱스만 전송
                        self.client_socket.sendall(f"{self.worker_id} 성공: C[{i}, {j}]".encode('utf-8'))
                        self.success_count += 1
                    else:
                        raise Exception("Random failure occurred")
                except Exception as e:
                    # 작업 실패 시 Master Node에 실패 메시지 전송 (인덱스만)
                    print(f"{self.worker_id} failed to process task: {e}")
                    self.failure_count += 1
                    self.client_socket.sendall(f"{self.worker_id} 실패: C[{i}, {j}]".encode('utf-8'))

    def run(self):
        # Master Node와 연결
        self.connect_to_master()

        # 작업 수신 및 처리 스레드를 생성
        threading.Thread(target=self.receive_task).start()  # 작업 수신 스레드
        threading.Thread(target=self.process_task).start()  # 작업 처리 스레드

# 외부 통신을 위한 Worker Node
if __name__ == "__main__":
    worker_node = WorkerNode(master_host="127.0.0.1", master_port=9999)  # Google Cloud VM 외부 IP 주소
    worker_node.run()
