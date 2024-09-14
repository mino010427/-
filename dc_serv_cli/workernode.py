import threading
import time
import random
from queue import Queue
import socket

class WorkerNode:
    def __init__(self, worker_id, task_queue, worker_queues, log):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.worker_queues = worker_queues  # 다른 Worker 노드들의 큐
        self.log = log
        self.success_count = 0
        self.failure_count = 0
        self.total_task_time = 0
        self.task_count = 0
        self.start_time = time.time()

    def process_task(self):
        while not self.task_queue.empty():
            if self.task_count >= 10:
                self.log.append(f"Worker {self.worker_id}: 작업 할당 초과, 실패 처리")
                break

            task = self.task_queue.get()
            i, j, A_row, B_col = task
            try:
                # 1~3초의 랜덤 작업 시간
                task_time = random.uniform(1, 3)
                time.sleep(task_time)
                
                # 80% 확률로 성공, 20% 확률로 실패
                if random.random() < 0.8:
                    result = sum(a * b for a, b in zip(A_row, B_col))
                    self.success_count += 1
                    self.total_task_time += task_time
                    self.task_count += 1
                    self.log.append(f"Worker {self.worker_id} 성공: C[{i}, {j}] = {result}, 소요 시간: {task_time:.2f}초")
                    # 결과를 Master Node로 전송
                    self.send_result_to_master(i, j, result)
                else:
                    raise Exception(f"Worker {self.worker_id} 실패: C[{i}, {j}]")
            except Exception as e:
                self.failure_count += 1
                self.task_queue.put(task)  # 실패한 작업을 다시 큐에 넣음
                self.log.append(str(e))
            finally:
                self.task_queue.task_done()

    def send_result_to_master(self, i, j, result):
        # Master Node로 결과를 전송하는 예시
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('127.0.0.1', 9999))  # Master Node IP 및 포트
            sock.sendall(f"Result: C[{i},{j}] = {result}".encode('utf-8'))

    def attempt_task_transfer(self):
        # 다른 Worker Node에게 작업을 재할당 시도
        for queue in self.worker_queues:
            if queue.qsize() < 10:  # 대기열이 짧은 다른 Worker에게 넘김
                task = self.task_queue.get()
                queue.put(task)
                self.log.append(f"Worker {self.worker_id}: 작업을 다른 Worker로 재할당")
                return

    def print_statistics(self):
        total_time = time.time() - self.start_time
        avg_task_time = self.total_task_time / self.success_count if self.success_count else 0
        print(f"Worker {self.worker_id} 통계:")
        print(f"  성공한 작업 수: {self.success_count}")
        print(f"  실패한 작업 수: {self.failure_count}")
        print(f"  처리한 작업 수: {self.task_count}")
        print(f"  평균 작업 시간: {avg_task_time:.2f}초")
        print(f"  전체 수행 시간: {total_time:.2f}초")
