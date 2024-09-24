import multiprocessing
import numpy as np
import json
import time
import random

class MasterNode:
    def __init__(self):
        self.A = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 A
        self.B = np.random.randint(1, 100, (10, 10))  # 10x10 행렬 B
        self.C = np.zeros((10, 10))  # 결과 행렬 C
        self.task_queue = multiprocessing.Queue()  # 작업 큐
        self.failed_queue = multiprocessing.Queue()  # 실패한 작업 큐
        self.num_workers = 4  # Worker Node의 개수
        self.workers = []  # Worker 프로세스를 저장할 리스트
        self.parent_pipes = []  # 부모 파이프 저장

    def add_tasks_to_queue(self):
        """A와 B의 곱셈 작업을 task_queue에 추가"""
        for i in range(10):
            for j in range(10):
                task_data = {'i': i, 'j': j, 'A_row': self.A[i, :].tolist(), 'B_col': self.B[:, j].tolist()}
                self.task_queue.put(json.dumps(task_data))

    def distribute_tasks(self):
        """작업을 Worker에게 분배"""
        while not self.task_queue.empty() or not self.failed_queue.empty():
            for pipe in self.parent_pipes:
                if not self.task_queue.empty():
                    task_data = self.task_queue.get()
                    pipe.send(task_data)
                elif not self.failed_queue.empty():
                    failed_task = self.failed_queue.get()
                    pipe.send(failed_task)

    def receive_results(self):
        """Worker Node로부터 작업 결과를 수신"""
        for pipe in self.parent_pipes:
            while pipe.poll():  # 결과가 도착하면 처리
                result = pipe.recv()
                result_data = json.loads(result)

                i, j = result_data['i'], result_data['j']
                if 'failed' in result_data:
                    print(f"작업 실패: C[{i}, {j}]")
                    self.failed_queue.put(result)  # 실패한 작업 재할당
                else:
                    result_value = result_data['result']
                    self.C[i, j] = result_value  # 결과 행렬 업데이트
                    print(f"작업 성공: C[{i}, {j}] = {result_value}")

    def run(self):
        """Master Node 실행"""
        # Worker Node 프로세스 생성 및 파이프 설정
        for _ in range(self.num_workers):
            parent_pipe, child_pipe = multiprocessing.Pipe()
            worker_process = multiprocessing.Process(target=worker_node, args=(child_pipe,))
            worker_process.start()
            self.workers.append(worker_process)
            self.parent_pipes.append(parent_pipe)

        # 작업을 큐에 추가
        self.add_tasks_to_queue()

        # 메인 루프: 작업을 분배하고, 결과를 수신함
        while not self.task_queue.empty() or not self.failed_queue.empty() or any([worker.is_alive() for worker in self.workers]):
            self.distribute_tasks()  # 작업 분배
            self.receive_results()  # 결과 수신 및 실패 작업 처리

        # 결과 출력
        print("\n결과 행렬 C:")
        print(self.C)

def worker_node(pipe):
    """Worker Node 프로세스 함수"""
    while True:
        if pipe.poll():  # Master로부터 작업을 수신
            task_data = pipe.recv()
            task = json.loads(task_data)
            i, j = task['i'], task['j']
            A_row = task['A_row']
            B_col = task['B_col']

            # 행렬 곱 연산
            result_value = sum(a * b for a, b in zip(A_row, B_col))

            # 성공 확률 80%
            if random.random() < 0.8:
                result = json.dumps({'i': i, 'j': j, 'result': result_value})
            else:
                result = json.dumps({'i': i, 'j': j, 'failed': True})

            pipe.send(result)
            time.sleep(random.uniform(1, 2))  # 처리 시간 시뮬레이션

# Master Node 실행
if __name__ == "__main__":
    master_node = MasterNode()
    master_node.run()
