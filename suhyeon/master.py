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
        print(f"{worker_id}연결, {address}")

        # Worker에게 ID를 명시적으로 전송
        client_socket.sendall(worker_id.encode('utf-8'))

        # 4개 Worker Node가 연결될 때까지 대기
        while True:
            if self.connected_workers == 4:
                print("Worker Node 4개 연결, 작업 분배...")
                self.distribute_config()
                break

    def add_tasks_to_queue(self):
        # 모든 작업을 task_queue에 추가
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
            if not self.failed_queue.empty():  # 실패한 작업이 있다면 최우선으로 처리
                with self.lock:
                    failed_task_info = self.failed_queue.get()
                    i, j = map(int, failed_task_info.split("C[")[1].rstrip(']').split(', '))
                    A_row = self.A[i, :].tolist()
                    B_col = self.B[:, j].tolist()
                    task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})

                # 여기서 각 worker의 큐 상태를 확인한 후, 큐가 꽉 차지 않은 worker로만 작업을 보냄
                for worker_socket in self.worker_sockets:
                    if self.get_worker_queue_status(worker_socket) < 10:  # 큐가 꽉 차지 않았는지 확인
                        worker_socket.send((task_data + "<END>").encode('utf-8'))
                        print(f"작업실패 재할당: {self.worker_ids[worker_socket]} / C[{i}, {j}]")
                        break
            else:
                if not self.task_queue.empty():
                    with self.lock:
                        task_data = self.task_queue.get()

                    # 마찬가지로 worker의 큐 상태를 고려하여 작업을 전송
                    for worker_socket in self.worker_sockets:
                        if self.get_worker_queue_status(worker_socket) < 10:
                            worker_socket.send((task_data + "<END>").encode('utf-8'))
                            print(f"작업전송: {self.worker_ids[worker_socket]}")
            time.sleep(1)

    # 각 worker의 작업 큐 상태를 받는 메서드
    def get_worker_queue_status(self, worker_socket):
        worker_socket.send(b"QUEUE_STATUS")  # Worker에게 현재 큐 상태를 요청
        queue_status = worker_socket.recv(1024).decode()  # Worker로부터 큐 상태 수신
        return int(queue_status)  # 큐에 남은 작업 개수를 정수로 변환하여 반환

    def receive_results(self, worker_socket):
        # 각 Worker Node로부터 결과 수신 및 재할당 처리
        try:
            while True:
                result = worker_socket.recv(1024).decode()
                if result:
                    if "failed" in result:
                        # 작업 실패 시 재할당
                        task_data = result.split("failed task for C[")[1].split(']')[0]  # C[i, j]에서 i, j만 추출
                        i, j = task_data.split(', ')
                        print(f"작업실패: {self.worker_ids[worker_socket]} / C[{i}, {j}]")
                        
                        with self.lock:  # 작업 큐에 실패한 작업을 추가할 때 뮤텍스 잠금
                            self.failed_queue.put(f"C[{i}, {j}]")  # 실패한 작업을 실패 큐에 추가
                    else:
                        # 성공한 경우 성공한 행렬 인덱스만 출력
                        task_data = result.split("C[")[1].split(']')[0]  # C[i, j]에서 i, j만 추출
                        i, j = task_data.split(', ')
                        print(f"작업성공: {self.worker_ids[worker_socket]} / C[{i}, {j}]")
                time.sleep(1)  # 통신 지연 시뮬레이션
        except Exception as e:
            print(f"오류!: {self.worker_ids[worker_socket]} / {e}")


    def run(self):
        # 소켓 설정
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"Master Node 시작 {self.host}:{self.port}")

        # Worker Node의 접속을 기다림
        while self.connected_workers < 4:
            client_socket, address = server_socket.accept()
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))
            worker_thread.start()

        # 작업 추가를 위한 스레드 시작
        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()
