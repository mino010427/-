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
        self.M=4
        self.connected_workers = 0
        self.worker_ids = {}
        self.worker_status = {}
        self.A = np.random.randint(1, 100, (self.M, self.M))  
        self.B = np.random.randint(1, 100, (self.M, self.M))  
        self.task_queue = Queue()
        self.failed_queue = Queue()
        self.lock = threading.Lock()
        self.result_matrix = np.zeros((self.M, self.M))
        self.log_file = open("Master.txt", "w")
        self.stop_flag=True

    def log_event(self, message):
        timestamp = f"[{self.system_clock.get_elapsed_time():.2f}] "
        log_message = timestamp + message
        self.log_file.write(log_message + "\n")
        self.log_file.flush()
        print(log_message)

    def handle_worker(self, client_socket, address):
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        self.worker_status[worker_id] = {'queue_used': 0, 'queue_remaining': 10}
        self.log_event(f"{worker_id} 연결, {address}")

        client_socket.sendall((worker_id + "<END>").encode('utf-8'))

        if self.connected_workers == 4:
            self.log_event("Worker Node 4개 연결 완료, 작업 분배 시작...")

    def distribute_tasks(self):
        while self.stop_flag:
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
        for i in range(self.M):
            for j in range(self.M):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                self.task_queue.put(task_data)
        self.log_event("모든 작업을 작업 큐에 추가 완료")

    def find_load_worker(self):
        max_remaining = -1
        selected_worker = None

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
            while self.stop_flag:
                result = worker_socket.recv(1024).decode()
                buffer += result
                if "<END>" in buffer:
                    complete_result = buffer.split("<END>")[0]
                    buffer = buffer.split("<END>")[1]

                    result_data = json.loads(complete_result)

                    worker_id = result_data["worker_id"]
                    queue_remaining = result_data["queue_remaining"]
                    self.worker_status[worker_id]['queue_remaining'] = queue_remaining
                    self.worker_status[worker_id]['queue_used'] = 10 - queue_remaining

                    if result_data["status"] == "failed":
                        task_data = result_data["task"].split("C[")[1].split(']')[0]
                        i, j = map(int, task_data.split(', '))
                        self.log_event(f"작업 실패: {worker_id} / C[{i}, {j}]")

                        with self.lock:
                            self.result_matrix[i, j] = -1
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
                            
                self.check_completion()

        

        except Exception as e:
            self.log_event(f"오류 발생: {self.worker_ids[worker_socket]} / {e}")


    def check_completion(self):
        with self.lock:
            # 모든 worker의 queue_remaining 값이 10인지 확인
            all_workers_idle = all(status['queue_remaining'] == 10 for status in self.worker_status.values())
            
            # task_queue와 failed_queue가 모두 비어 있는지 확인
            no_pending_tasks = self.task_queue.empty() and self.failed_queue.empty()
            
            # 두 조건이 모두 만족하면 stop_flag를 False로 설정
            if all_workers_idle and no_pending_tasks:
                self.stop_flag = False
                # 모든 Worker에게 "complete<END>" 메시지를 전송
                for worker_socket in self.worker_sockets:
                    try:
                        worker_socket.send("complete<END>".encode('utf-8'))
                        self.log_event(f"Worker Node {self.worker_ids[worker_socket]}에게 complete 메시지 전송.")
                    except Exception as e:
                        self.log_event(f"Worker Node {self.worker_ids[worker_socket]}에게 complete 메시지 전송 실패: {e}")
            
                self.log_event("모든 작업 완료. Master Node 종료 준비 중.")
                


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
        
        distribution_thread.join()
     
        
        self.log_event("모든 작업 완료. 최종 행렬:")
        self.log_event(f"\n{self.result_matrix}")
        print(self.result_matrix)
        

if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=5000)
    master_node.run()
