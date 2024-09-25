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
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.system_clock = SystemClock()
        self.worker_sockets = []
        self.connected_workers = 0  # 접속한 Worker Node 수
        self.worker_ids = {}  # Worker ID 매핑
        self.worker_status = {}  # 각 Worker Node의 큐 상태를 저장할 딕셔너리
        self.A = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 A
        self.B = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 B
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐 추가
        self.lock = threading.Lock()  # 뮤텍스 추가
        self.result_matrix = np.zeros((10, 10))  # 10x10 결과 행렬 초기화

        # 로그 파일 초기화
        self.log_file = open("Master.txt", "w")

    def log_event(self, message):
        # 로그를 파일과 터미널에 출력
        timestamp = f"[{self.system_clock.get_elapsed_time():.2f}] "
        log_message = timestamp + message
        self.log_file.write(log_message + "\n")
        self.log_file.flush()  # 즉시 로그 기록
        print(log_message)

    def handle_worker(self, client_socket, address):
        # Worker Node를 연결 목록에 추가 및 ID 할당
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        self.worker_status[worker_id] = {'queue_used': 0, 'queue_remaining': 10}  # 초기 큐 상태 저장
        self.log_event(f"{worker_id} 연결, {address}")

        # Worker에게 ID를 명시적으로 전송
        client_socket.sendall((worker_id + "<END>").encode('utf-8'))

        if self.connected_workers == 4:
            self.log_event("Worker Node 4개 연결 완료, 작업 분배 시작...")

    def distribute_tasks(self):
        buffer = ""
        while True:
            if not self.failed_queue.empty():
                with self.lock:
                    failed_task_info = self.failed_queue.get()
                    i, j = map(int, failed_task_info.split("C[")[1].rstrip(']').split(', '))
                    A_row = self.A[i, :].tolist()
                    B_col = self.B[:, j].tolist()
                    task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})

                while self.worker_status_all_full():
                    time.sleep(1)

                selected_worker_id = self.find_load_worker()
                for worker_socket, worker_id in self.worker_ids.items():
                    if worker_id == selected_worker_id:
                        worker_socket.send((task_data + "<END>").encode('utf-8'))
                        self.log_event(f"실패 작업 재전송: {worker_id} / C[{i}, {j}]")
                        break
            else:
                if not self.task_queue.empty():
                    task_data = self.task_queue.get()

                    while self.worker_status_all_full():
                        time.sleep(1)

                    selected_worker_id = self.find_load_worker()
                    for worker_socket, worker_id in self.worker_ids.items():
                        if worker_id == selected_worker_id:
                            worker_socket.send((task_data + "<END>").encode('utf-8'))
                            self.log_event(f"작업 전송: {worker_id}")
                            break

                time.sleep(0.5)

    def add_tasks_to_queue(self):
        for i in range(10):
            for j in range(10):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                self.task_queue.put(task_data)
        self.log_event("모든 작업을 작업 큐에 추가 완료")

    def find_load_worker(self):
        max_remaining = -1  # 가장 큰 queue_remaining 값을 저장할 변수
        selected_worker = None  # 선택된 Worker의 ID를 저장할 변수

        for worker_id, status in self.worker_status.items():
            remaining = status['queue_remaining']
            if remaining > max_remaining:
                max_remaining = remaining
                selected_worker = worker_id
            elif remaining == max_remaining and worker_id < selected_worker:
                selected_worker = worker_id

        return selected_worker

    def worker_status_all_full(self):
        for worker_id, status in self.worker_status.items():
            if status['queue_remaining'] > 0:
                return False
        return True

    def receive_results(self, worker_socket):
        buffer = ""
        try:
            while True:
                result = worker_socket.recv(1024).decode()
                buffer += result
                if "<END>" in buffer:
                    complete_result = buffer.split("<END>")[0]
                    buffer = buffer.split("<END>")[1]  # 남은 데이터는 버퍼에 저장

                    result_data = json.loads(complete_result)

                    worker_id = result_data["worker_id"]
                    queue_remaining = result_data["queue_remaining"]
                    self.worker_status[worker_id]['queue_remaining'] = queue_remaining
                    self.worker_status[worker_id]['queue_used'] = 10 - queue_remaining

                    if result_data["status"] == "failed":
                        task_data = result_data["task"].split("C[")[1].split(']')[0]  # C[i, j]에서 i, j 추출
                        i, j = map(int, task_data.split(', '))
                        self.log_event(f"작업 실패: {worker_id} / C[{i}, {j}]")

                        with self.lock:
                            self.result_matrix[i, j] = -1  # 실패한 작업은 -1로 저장
                            self.failed_queue.put(f"C[{i}, {j}]")

                    elif result_data["status"] == "received":
                        self.log_event(f"작업 수신 성공: {worker_id} - 남은 큐 공간: {queue_remaining}")

                    elif result_data["status"] == "success":
                        task_data = result_data["task"].split("C[")[1].split(']')[0]
                        i, j = map(int, task_data.split(', '))
                        result_value = float(result_data["result"])
                        self.log_event(f"작업 성공: {worker_id} / C[{i}, {j}] = {result_value}")

                        with self.lock:
                            self.result_matrix[i, j] = result_value

                    with self.lock:
                        if np.all(self.result_matrix != 0):
                            self.log_event("모든 작업 완료. 최종 행렬:")
                            print(self.result_matrix)
                            break

        except Exception as e:
            self.log_event(f"오류 발생: {self.worker_ids[worker_socket]} / {e}")

    def run(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        self.log_event(f"Master Node 시작 {self.host}:{self.port}")

        while self.connected_workers < 4:
            client_socket, address = server_socket.accept()
            self.handle_worker(client_socket, address)
            threading.Thread(target=self.receive_results, args=(client_socket,)).start()

        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

        distribution_thread = threading.Thread(target=self.distribute_tasks)
        distribution_thread.start()

if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=5000)
    master_node.run()
