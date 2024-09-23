import threading
import queue
import random
import time
import logging

# 로그 설정
logging.basicConfig(filename='master.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

class MasterNode:
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.matrix_size = 1000
        self.A = [[random.randint(1, 99) for _ in range(self.matrix_size)] for _ in range(self.matrix_size)]
        self.B = [[random.randint(1, 99) for _ in range(self.matrix_size)] for _ in range(self.matrix_size)]
        self.C = [[0 for _ in range(self.matrix_size)] for _ in range(self.matrix_size)]
        self.task_queue = queue.Queue()
        self.failed_tasks = queue.PriorityQueue()  # 우선순위가 있는 실패 작업 큐
        self.lock = threading.Lock()  # 행렬 업데이트를 위한 락
        self.system_clock = time.time()

        # 각 워커에게 작업을 보낼 스레드 생성
        self.workers = [WorkerConnection(self, i) for i in range(self.num_workers)]

    def distribute_tasks(self):
        logging.info("작업 분배 시작")
        for i in range(self.matrix_size):
            for j in range(self.matrix_size):
                task = (i, j)  # C[i, j]를 계산하는 작업
                self.task_queue.put(task)

        # 워커에게 작업 할당
        threads = []
        for worker in self.workers:
            t = threading.Thread(target=worker.start)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def update_matrix(self, i, j, result):
        with self.lock:
            self.C[i][j] = result
            logging.info(f"작업 완료: C[{i}, {j}] = {result}")

    def task_failed(self, task):
        with self.lock:
            self.failed_tasks.put(task)
            logging.info(f"작업 실패: {task} 재할당 예정")

    def reallocate_failed_tasks(self):
        logging.info("실패 작업 재할당 시작")
        while not self.failed_tasks.empty():
            task = self.failed_tasks.get()
            self.task_queue.put(task)
    
    def print_results(self):
        total_time = time.time() - self.system_clock
        logging.info(f"총 연산 소요 시간: {total_time:.2f}초")
        print(f"총 연산 소요 시간: {total_time:.2f}초")
        print("최종 행렬 C 출력:")
        for row in self.C:
            print(row)


class WorkerConnection:
    def __init__(self, master, worker_id):
        self.master = master
        self.worker_id = worker_id
        self.queue = queue.Queue(maxsize=10)

    def start(self):
        while not self.master.task_queue.empty():
            if self.queue.qsize() < 10:
                try:
                    task = self.master.task_queue.get_nowait()
                    self.process_task(task)
                except queue.Empty:
                    continue
            else:
                time.sleep(1)  # 큐가 가득 차면 대기

    def process_task(self, task):
        i, j = task
        success = random.random() < 0.8  # 80% 성공 확률
        time.sleep(random.randint(1, 3))  # 작업 소요 시간 1~3초
        if success:
            result = sum(self.master.A[i][k] * self.master.B[k][j] for k in range(self.master.matrix_size))
            self.master.update_matrix(i, j, result)
        else:
            self.master.task_failed(task)
            logging.info(f"작업 실패: {task}, Worker {self.worker_id}")


if __name__ == '__main__':
    master_node = MasterNode(num_workers=4)
    master_node.distribute_tasks()
    master_node.reallocate_failed_tasks()
    master_node.print_results()
