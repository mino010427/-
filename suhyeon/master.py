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
        self.worker_status_dictionary = {}  # Worker queue상태 딕셔너리
        self.A = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 A
        self.B = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 B
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐 추가
        self.lock = threading.Lock()  # 뮤텍스 추가
        self.total_tasks = 10 * 10  # 총 작업 수
        self.completed_tasks = 0  # 완료된 작업 수
    
    def handle_worker(self, client_socket, address):
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        print(f"{worker_id}연결, {address}")
        client_socket.sendall(worker_id.encode('utf-8'))

        # Worker Node의 큐 상태를 주기적으로 수신
        threading.Thread(target=self.receive_worker_status, args=(client_socket, worker_id)).start()

        while True:
            if self.connected_workers == 4:
                print("Worker Node 4개 연결, 작업 분배 시작...")
                self.distribute_config()
                break

    def receive_worker_status(self, client_socket, worker_id):
        while True:
            try:
                status_data = client_socket.recv(1024).decode()
                if status_data:
                    status = json.loads(status_data)
                    self.worker_status_dictionary[worker_id] = status
                    print(f"Worker {worker_id} 상태 - 큐 사용 중: {status['queue_used']}, 남은 공간: {status['queue_remaining']}")
            except Exception as e:
                print(f"Worker {worker_id} 상태 수신 오류: {e}")
                break

    def distribute_config(self):
        # 각 Worker Node가 작업을 수신할 수 있도록 스레드를 시작함
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()

        # 작업 분배를 위한 스레드 시작
        distribution_thread = threading.Thread(target=self.distribute_tasks)
        distribution_thread.start()

    def add_tasks_to_queue(self):
        # 모든 작업을 task_queue에 추가
        for i in range(1000):
            for j in range(1000):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                
                with self.lock:  # 큐에 접근할 때 뮤텍스 잠금
                    self.task_queue.put(task_data)

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



    def distribute_tasks(self):
        while True:
            if self.completed_tasks >= self.total_tasks:
                print(f"모든 작업 완료. 종료 중... System clock: {self.system_clock.get_elapsed_time()}")
                self.terminate_workers()
                break

            # 실패한 작업 우선 분배
            if not self.failed_queue.empty():
                failed_task_info = self.failed_queue.get()
                i, j = map(int, failed_task_info.split("C[")[1].rstrip(']').split(', '))
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                
                available_worker = self.get_least_loaded_worker()
                if available_worker:
                    available_worker.send((task_data + "<END>").encode('utf-8'))
                    print(f"재분배: {self.worker_ids[available_worker]} / C[{i}, {j}]")
            else:
                if not self.task_queue.empty():
                    task_data = self.task_queue.get()
                    available_worker = self.get_least_loaded_worker()
                    if available_worker:
                        available_worker.send((task_data + "<END>").encode('utf-8'))
                        print(f"작업 전송: {self.worker_ids[available_worker]}")
            
            time.sleep(1)

    def get_least_loaded_worker(self):
        # 가장 여유 있는 Worker Node를 찾음
        least_loaded_worker = None
        least_queue_used = float('inf')

        for worker_socket in self.worker_sockets:
            worker_id = self.worker_ids[worker_socket]
            if worker_id in self.worker_status_dictionary:
                queue_used = self.worker_status_dictionary[worker_id]['queue_used']
                if queue_used < least_queue_used:
                    least_loaded_worker = worker_socket
                    least_queue_used = queue_used

        return least_loaded_worker

    def terminate_workers(self):
        # 모든 Worker에게 종료 메시지 전송
        for worker_socket in self.worker_sockets:
            worker_socket.send("TERMINATE<END>".encode('utf-8'))
        print("모든 Worker 노드 종료 명령 전송 완료.")

    def run(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Master Node 시작 {self.host}:{self.port}")

        while self.connected_workers < 4:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_worker, args=(client_socket, address)).start()

        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)
    master_node.run()
