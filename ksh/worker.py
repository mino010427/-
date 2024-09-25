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
        self.log_file = None

    def log_event(self, message):
        timestamp = f"[{self.system_clock.get_elapsed_time():.2f}] "
        log_message = timestamp + message
        self.log_file.write(log_message + "\n")
        self.log_file.flush()
        print(log_message)

    def connect_to_master(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.master_host, self.master_port))
        print(f"Master Node와 연결 {self.master_host}:{self.master_port}")

        self.worker_id = ""
        buffer = ""
        while True:
            data = self.client_socket.recv(1024).decode()
            buffer += data
            if "<END>" in buffer:
                self.worker_id = buffer.split("<END>")[0]
                break
        
        self.log_file = open(f"{self.worker_id}.txt", "w")
        self.log_event(f"Worker ID 할당: {self.worker_id}")

    def receive_task(self):
        buffer = ""
        self.log_event('Master Node로부터 작업 수신 시작')
        while True:
            try:
                task_data = self.client_socket.recv(1024).decode()
                if task_data:
                    buffer += task_data
                    if "<END>" in buffer:
                        complete_task = buffer.split("<END>")[0]
                        buffer = buffer.split("<END>")[1]

                        try:
                            task_json = json.loads(complete_task)
                            i, j = task_json['i'], task_json['j']
                        except (json.JSONDecodeError, KeyError) as e:
                            self.log_event(f"작업 데이터 파싱 오류: {e}")
                            continue

                        try:
                            self.task_queue.put(complete_task, timeout=1)
                            self.log_event(f"작업 수신 성공: {self.worker_id} / C[{i}, {j}]")

                            queue_remaining = self.task_queue.maxsize - self.task_queue.qsize()
                            success_message = json.dumps({
                                "worker_id": self.worker_id,
                                "status": "received",
                                "task": f"C[{i}, {j}]",
                                "queue_remaining": queue_remaining
                            }) + "<END>"

                            self.client_socket.sendall(success_message.encode('utf-8'))

                        except Full:
                            self.log_event(f"작업 실패: {self.worker_id}의 큐가 가득 참 C[{i},{j}]")
                            self.failure_count += 1

                            queue_remaining = self.task_queue.maxsize - self.task_queue.qsize()

                            failure_message = json.dumps({
                                "worker_id": self.worker_id,
                                "status": "failed",
                                "task": f"C[{i}, {j}]",
                                "queue_remaining": queue_remaining
                            }) + "<END>"

                            self.client_socket.sendall(failure_message.encode('utf-8'))

            except Exception as e:
                self.log_event(f"Error receiving task: {e}")
                break

    def process_task(self):
        while True:
            if not self.task_queue.empty():
                task_data = self.task_queue.get()

                task = json.loads(task_data)
                i, j, A_row, B_col = task['i'], task['j'], task['A_row'], task['B_col']

                self.log_event(f"작업 처리: {self.worker_id} / C[{i}, {j}]")

                try:
                    time.sleep(random.uniform(1, 3))
                    result = sum(a * b for a, b in zip(A_row, B_col))

                    queue_remaining = self.task_queue.maxsize - self.task_queue.qsize()

                    if random.random() < 0.8:
                        success_message = json.dumps({
                            "worker_id": self.worker_id,
                            "status": "success",
                            "task": f"C[{i}, {j}]",
                            "result": result,
                            "queue_remaining": queue_remaining
                        }) + "<END>"

                        self.log_event(f"{self.worker_id} 작업 성공: C[{i}, {j}] = {result}")
                        self.client_socket.sendall(success_message.encode('utf-8'))
                        self.success_count += 1
                    else:
                        raise Exception("Random failure occurred")

                except Exception as e:
                    queue_remaining = self.task_queue.maxsize - self.task_queue.qsize()
                    failure_message = json.dumps({
                        "worker_id": self.worker_id,
                        "status": "failed",
                        "task": f"C[{i}, {j}]",
                        "queue_remaining": queue_remaining
                    }) + "<END>"

                    self.client_socket.sendall(failure_message.encode('utf-8'))
                    self.log_event(f"{self.worker_id} 작업 실패: C[{i}, {j}], {e}")
                    self.failure_count += 1

    def finalize_and_log(self):
        total_tasks = self.success_count + self.failure_count
        avg_waiting_time = self.system_clock.get_elapsed_time() / total_tasks if total_tasks > 0 else 0

        # 로그 기록 확인용 메시지 추가
        self.log_event("작업 완료 후 로그 기록 시작")

        # 연산 성공 및 실패 횟수 로그 기록
        self.log_event(f"연산 성공 횟수: {self.success_count}, 실패 횟수: {self.failure_count}")
        
        # 작업 처리량 및 평균 대기시간 로그 기록
        self.log_event(f"작업 처리량: {total_tasks}, 평균 대기시간: {avg_waiting_time:.2f}초")
        
        # 전체 수행시간 로그 기록
        self.log_event(f"전체 수행시간: {self.system_clock.get_elapsed_time():.2f}초")
        
        # 로그 기록 완료 확인 메시지
        self.log_event("작업 완료 후 로그 기록 완료")

    def run(self):
        self.connect_to_master()

        # 작업 수신 및 처리 스레드 시작
        receive_thread = threading.Thread(target=self.receive_task)
        receive_thread.start()

        process_thread = threading.Thread(target=self.process_task)
        process_thread.start()

        # 스레드 종료 대기
        receive_thread.join()
        process_thread.join()

        # 스레드가 종료된 후 로그 기록
        self.finalize_and_log()
        self.log_event("작업 완료 후 로그 기록이 실행되었습니다.")


if __name__ == "__main__":
    worker_node = WorkerNode(master_host="127.0.0.1", master_port=5000)
    worker_node.run()
