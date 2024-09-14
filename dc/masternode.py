import threading
import time
import random
import numpy as np
from queue import Queue
#남태인 왔다감
# 행렬 크기 및 작업 큐 설정
N = 1000
A = np.random.randint(1, 100, (N, N))
B = np.random.randint(1, 100, (N, N))
C = np.zeros((N, N))
task_queue = Queue()

# Worker Node를 담당하는 스레드
def worker_node(worker_id):
    task_count = 0  # 각 워커 노드의 작업 수를 관리하는 변수

    while not task_queue.empty():
        if task_count >= 10:  # 할당된 작업이 10개를 넘으면 중단
            print(f"Worker {worker_id}: 작업 할당 초과, 실패 처리")
            break

        task = task_queue.get()
        i, j = task
        try:
            # 1~3초 동안 무작위로 시간이 소요됨
            task_time = random.uniform(1, 3)
            time.sleep(task_time)
            
            # 80% 확률로 성공, 20% 확률로 실패
            if random.random() < 0.8:
                C[i, j] = sum(A[i, k] * B[k, j] for k in range(N))
                print(f"Worker {worker_id} 성공: C[{i}, {j}] = {C[i, j]}")
                task_count += 1  # 작업 성공 시 카운트 증가
            else:
                raise Exception(f"Worker {worker_id} 실패: C[{i}, {j}]")
        except Exception as e:
            print(e)
            task_queue.put(task)  # 실패한 작업을 다시 큐에 넣음
        finally:
            task_queue.task_done()

# Master Node 역할: 작업 분배 및 관리
def master_node():
    # 작업 분배 (C[i, j] 계산 작업)
    for i in range(N):
        for j in range(N):
            task_queue.put((i, j))

    # 스레드 풀 생성
    threads = []
    for worker_id in range(10):  # 10개의 Worker Node 스레드
        thread = threading.Thread(target=worker_node, args=(worker_id,))
        threads.append(thread)
        thread.start()

    # 모든 스레드가 작업을 완료할 때까지 대기
    for thread in threads:
        thread.join()

# System Clock 클래스
class SystemClock:
    def __init__(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

# System Clock 시작
system_clock = SystemClock()

# Master Node 실행
master_node()

# 총 소요 시간 출력
print(f"Total elapsed time: {system_clock.get_elapsed_time()} seconds")
