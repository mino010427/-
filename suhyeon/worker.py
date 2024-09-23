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
        self.worker_id = None
        self.master_host = master_host
        self.master_port = master_port
        self.system_clock = SystemClock()
        self.task_queue = Queue(maxsize=10)
        self.success_count = 0
        self.failure_count = 0

    def connect_to_master(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Master Node와 연결 {self.master_host}:{self.master_port}")
        self.worker_id = self.client_socket.recv(1024).decode()
        print(f"Worker ID 할당: {self.worker_id}")

    def report_queue_status(self):
        try:
            queue_size = self.task_queue.qsize() # 현재 큐에 있는 작업 개수
            max_size = self.task_queue.maxsize # 큐의 최대 크기
            queue_remaining = max_size - queue_size # 남은 큐 공간 계산
            
            # 작업 큐 상태 전송 (현재 작업 개수와 남은 공간)
            queue_status = {
                'worker_id': self.worker_id,
                'queue_used': queue_size, # 현재 큐에 있는 작업 개수
                'queue_remaining': queue_remaining # 남은 큐 공간
            }
            self.client_socket.sendall(json.dumps(queue_status).encode('utf-8'))
            print(f"{self.worker_id} 큐 상태 보고 - 사용 중: {queue_size}, 남은 공간: {queue_remaining}")
        except Exception as e:
            print(f"큐 상태 보고 오류: {e}")

    def receive_task(self):
        buffer = ""
        print('Master Node로부터 작업 수신 시작')
        while True:
            try:
                task_data = self.client_socket.recv(1024).decode()
                if task_data == "TERMINATE<END>":
                    print(f"{self.worker_id} 종료")
                    break
                buffer += task_data
                if "<END>" in buffer:
                    complete_task = buffer.split("<END>")[0]
                    buffer = buffer.split("<END>")[1]
                    try:
                        self.task_queue.put(complete_task, timeout=1)
                        print(f"작업 수신: {self.worker_id}")
                        self.process_task()
                        self.report_queue_status()
                    except Full:
                        print(f"작업 실패: {self.worker_id}의 큐가 가득 참")
                        self.client_socket.sendall(f"{self.worker_id} failed to receive task".encode('utf-8'))
                        self.report_queue_status()
            except Exception as e:
                print(f"작업 수신 오류: {e}")
                break

    def process_task(self):
        if not self.task_queue.empty():
            task_data = self.task_queue.get()
            task = json.loads(task_data)
            i, j, A_row, B_col = task['i'], task['j'], task['A_row'], task['B_col']
            print(f"작업 처리: {self.worker_id} / C[{i}, {j}]")
            try:
                time.sleep(random.uniform(1, 3))
                if random.random() < 0.8:
                    self.client_socket.sendall(f"{self.worker_id} 성공: C[{i}, {j}]".encode('utf-8'))
                    self.success_count += 1
                else:
                    raise Exception("랜덤 실패")
            except Exception as e:
                print(f"{self.worker_id} 작업 실패: C[{i}, {j}], {e}")
                self.client_socket.sendall(f"{self.worker_id} failed task for C[{i}, {j}]".encode('utf-8'))
                self.failure_count += 1
            self.report_queue_status()

    def run(self):
        self.connect_to_master()
        threading.Thread(target=self.receive_task).start()

if __name__ == "__main__":
    worker_node = WorkerNode(master_host="127.0.0.1", master_port=9999)
    worker_node.run()
