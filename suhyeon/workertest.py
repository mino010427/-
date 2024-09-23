import random
import time
import logging

# 로그 설정
logging.basicConfig(filename='worker.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

class WorkerNode:
    def __init__(self, worker_id, master):
        self.worker_id = worker_id
        self.master = master
        self.success_count = 0
        self.failure_count = 0
        self.start_time = time.time()

    def receive_task(self, task):
        i, j = task
        success = random.random() < 0.8  # 80% 성공 확률
        time.sleep(random.randint(1, 3))  # 작업 소요 시간 1~3초
        if success:
            result = self.compute_result(i, j)
            logging.info(f"Worker {self.worker_id} 작업 성공: {task}")
            return result
        else:
            logging.info(f"Worker {self.worker_id} 작업 실패: {task}")
            return None

    def compute_result(self, i, j):
        result = sum(self.master.A[i][k] * self.master.B[k][j] for k in range(self.master.matrix_size))
        return result

    def log_statistics(self):
        total_time = time.time() - self.start_time
        logging.info(f"Worker {self.worker_id} 총 수행 시간: {total_time:.2f}초")
        logging.info(f"Worker {self.worker_id} 작업 성공: {self.success_count}, 작업 실패: {self.failure_count}")
