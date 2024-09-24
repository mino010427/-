import socket
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
        self.worker_queues = []  # Worker 작업 큐 저장 리스트


    def start(self, host='0.0.0.0', port=9999):
        """Master Node를 외부 서버에서 실행"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(self.num_workers)
        print(f"Master Node가 {host}:{port}에서 시작되었습니다.")
        
        # Worker Node들과 연결
        for _ in range(self.num_workers):
            client_socket, address = server_socket.accept()
            print(f"Worker Node가 연결되었습니다: {address}")
            self.worker_sockets.append(client_socket)

        # 작업 분배 시작
        self.add_tasks_to_queue()
        self.distribute_tasks()

        # 결과 수신
        while any([worker for worker in self.worker_sockets]):
            self.receive_results()

        # 결과 출력
        print("\n결과 행렬 C:")
        print(self.C)


    def add_tasks_to_queue(self):
        """A와 B의 곱셈 작업을 task_queue에 추가"""
        for i in range(10):
            for j in range(10):
                task_data = {'i': i, 'j': j, 'A_row': self.A[i, :].tolist(), 'B_col': self.B[:, j].tolist()}
                self.task_queue.put(json.dumps(task_data))

    def distribute_tasks(self):
        """작업을 Worker에게 분배"""
        retry_limit = 3  # 실패한 작업에 대한 재시도 횟수 제한
        failed_task_attempts = {}  # 실패한 작업에 대한 시도 횟수 기록

        while not self.task_queue.empty() or not self.failed_queue.empty():
            for i, queue in enumerate(self.worker_queues):
                # **실패한 작업을 우선적으로 처리**
                if not self.failed_queue.empty() and not queue.full():
                    failed_task = self.failed_queue.get()  # 실패한 작업을 재분배
                    task = json.loads(failed_task)  # JSON 형식에서 Python 객체로 변환
                    task_key = (task['i'], task['j'])  # 작업 인덱스를 키로 사용
                    failed_task_attempts[task_key] = failed_task_attempts.get(task_key, 0) + 1

                    if failed_task_attempts[task_key] <= retry_limit:
                        print(f"Worker {i+1} - 재분배 실패 작업: C[{task['i']}, {task['j']}] (시도 {failed_task_attempts[task_key]}/{retry_limit})")
                        queue.put(failed_task)
                    else:
                        print(f"작업 재시도 한도 초과: C[{task['i']}, {task['j']}]")
                elif not self.task_queue.empty() and not queue.full():
                    task_data = self.task_queue.get()  # 새 작업 분배
                    task = json.loads(task_data)  # 새 작업을 처리할 때도 task로 변환
                    print(f"Worker {i+1} - 새 작업 분배: C[{task['i']}, {task['j']}]")
                    queue.put(task_data)

            # 모든 큐가 가득 찼다면 대기
            if all(queue.full() for queue in self.worker_queues):
                print("모든 Worker의 큐가 가득 찼습니다. 대기 중...")
                time.sleep(1)  # 잠시 대기

    def receive_results(self):
        """Worker Node로부터 작업 결과를 수신"""
        for worker_socket in self.worker_sockets:
            result = worker_socket.recv(1024).decode()
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
        # **자식 프로세스(Worker Node)를 생성하고 파이프 및 작업 큐 설정**
        for i in range(self.num_workers):
            parent_pipe, child_pipe = multiprocessing.Pipe()
            worker_queue = multiprocessing.Queue(maxsize=10)  # 큐의 최대 크기를 10으로 설정
            worker_process = multiprocessing.Process(target=worker_node, args=(child_pipe, worker_queue, i+1))
            worker_process.start()  # 자식 프로세스 시작
            self.workers.append(worker_process)
            self.parent_pipes.append(parent_pipe)
            self.worker_queues.append(worker_queue)

        # 작업을 큐에 추가하고 분배 시작
        self.add_tasks_to_queue()

        while any([worker.is_alive() for worker in self.workers]):
            self.distribute_tasks()  # 실패한 작업도 포함해서 바로 재분배
            self.receive_results()

        # 결과 출력
        print("\n결과 행렬 C:")
        print(self.C)

def worker_node(pipe, task_queue, worker_id):
    """Worker Node 프로세스 함수"""
    while True:
        if not task_queue.empty():  # 작업 큐에서 작업 수신
            task_data = task_queue.get()  # 작업 수신
            try:
                task = json.loads(task_data)
            except json.JSONDecodeError as e:
                print(f"작업 데이터 수신 오류: {e}")
                continue

            i, j = task['i'], task['j']
            A_row = task.get('A_row')
            B_col = task.get('B_col')

            if A_row is None or B_col is None:
                print(f"Worker {worker_id} - 작업 데이터 불량: {task_data}")
                continue

            # **작업 수신 출력**
            print(f"Worker {worker_id} - 작업 수신: C[{i}, {j}]")

            # 행렬 곱 연산
            result_value = sum(a * b for a, b in zip(A_row, B_col))

            # 성공 확률 80%
            if random.random() < 0.8:
                result = json.dumps({'i': i, 'j': j, 'result': result_value})
                print(f"Worker {worker_id} - 작업 성공: C[{i}, {j}] = {result_value}")
            else:
                result = json.dumps({'i': i, 'j': j, 'failed': True})
                print(f"Worker {worker_id} - 작업 실패: C[{i}, {j}]")

            pipe.send(result)  # 결과 전송
            time.sleep(random.uniform(1, 2))  # 처리 시간 시뮬레이션

# Master Node 실행
if __name__ == "__main__":
    master_node = MasterNode()
    master_node.run()
