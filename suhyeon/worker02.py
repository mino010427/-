import socket
import random
import numpy as np
import time
import json
from queue import Queue
import threading

class SystemClock:
    def __init__(self):
        self.start_time = time.time()  # 시작 시간 초기화

    def get_elapsed_time(self):
        return time.time() - self.start_time  # 경과 시간 반환

class WorkerNode:
    def __init__(self, master_host, master_port):
        self.worker_id = f"worker{random.randint(1, 100)}"  # 랜덤 워커 ID 생성
        self.master_host = master_host  # 마스터 노드 호스트
        self.master_port = master_port  # 마스터 노드 포트
        self.system_clock = SystemClock()  # 시스템 클락 객체 생성
        self.task_queue = Queue(maxsize=10)  # 작업 큐, 최대 크기 10
        self.success_count = 0  # 성공 횟수
        self.failure_count = 0  # 실패 횟수
        self.total_wait_time = 0  # 총 대기 시간
        self.total_tasks = 0  # 총 작업 수
        self.log_file = open(f"Worker_{self.worker_id}.txt", "w")  # 로그 파일 오픈

    def connect_to_master(self):
        """ 마스터 노드에 연결하는 함수. """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.master_host, self.master_port))  # 마스터 노드에 연결
        print(f"{self.worker_id} 마스터 노드에 연결됨.")

    def receive_task(self):
        """ 마스터 노드로부터 작업을 수신하는 함수. """
        while True:
            try:
                task_data = self.socket.recv(1024).decode()  # 작업 수신
                if task_data:
                    self.process_task(task_data)  # 수신한 작업 처리
            except Exception as e:
                print(f"{self.worker_id} 작업 수신 중 오류 발생: {e}")
                break

    def process_task(self, task_data):
        """ 수신한 작업을 처리하는 함수. """
        task_info = json.loads(task_data)  # 작업 정보를 JSON 형식으로 변환
        A_row = np.array(task_info['A_row'])  # A 행렬의 i번째 행
        B_col = np.array(task_info['B_col'])  # B 행렬의 j번째 열
        
        # 연산 소요 시간 계산
        computation_time = random.randint(1, 3)
        time.sleep(computation_time)  # 무작위 대기
        
        self.total_tasks += 1  # 총 작업 수 증가
        is_successful = random.random() < 0.8  # 80% 성공 확률

        if is_successful:  # 성공적인 경우
            result_value = np.dot(A_row, B_col)  # 행렬 곱셈 결과
            self.success_count += 1  # 성공 횟수 증가
            self.socket.send(json.dumps({'i': task_info['i'], 'j': task_info['j'], 'value': result_value}).encode('utf-8'))  # 결과 전송
            print(f"{self.worker_id} 작업 성공: C[{task_info['i']},{task_info['j']}] = {result_value}")
            self.log_file.write(f"{self.worker_id} 작업 성공: C[{task_info['i']},{task_info['j']}] = {result_value}\n")
        else:  # 실패한 경우
            self.failure_count += 1  # 실패 횟수 증가
            self.socket.send(f"failed task for {task_info}".encode('utf-8'))  # 실패 전송
            print(f"{self.worker_id} 작업 실패: C[{task_info['i']},{task_info['j']}] 계산 실패")
            self.log_file.write(f"{self.worker_id} 작업 실패: C[{task_info['i']},{task_info['j']}] 계산 실패\n")

    def log_results(self):
        """ 작업 종료 후 결과를 로그에 기록하는 함수. """
        print(f"{self.worker_id} 연산 성공 횟수: {self.success_count}")
        print(f"{self.worker_id} 연산 실패 횟수: {self.failure_count}")
        print(f"{self.worker_id} 총 작업 수: {self.total_tasks}")
        self.log_file.write(f"{self.worker_id} 연산 성공 횟수: {self.success_count}\n")
        self.log_file.write(f"{self.worker_id} 연산 실패 횟수: {self.failure_count}\n")
        self.log_file.write(f"{self.worker_id} 총 작업 수: {self.total_tasks}\n")
        self.log_file.close()  # 로그 파일 닫기

    def run(self):
        """ 워커 노드를 시작하고 마스터와의 연결을 관리하는 메인 함수. """
        self.connect_to_master()
        threading.Thread(target=self.receive_task).start()  # 작업 수신 스레드 시작
        # 이 부분은 remove, process_task 호출이 필요 없음
        # self.process_task()  # 작업 처리
        # 대신, 작업 수신이 되면 자동으로 처리됨
        # 결과 로그 출력은 수신 스레드 종료 후 호출
        self.log_results()  # 작업 종료 후 결과 로그 출력

# 워커 노드 실행
if __name__ == "__main__":
    worker_node = WorkerNode(master_host="34.68.170.234", master_port=9999)
    worker_node.run()
