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
    def __init__(self, host='0.0.0.0', port=9999):
        self.host = host
        self.port = port
        self.system_clock = SystemClock()
        self.worker_sockets = []
        self.connected_workers = 0  # 접속한 Worker Node 수
        self.worker_ids = {}  # Worker ID 매핑
        self.worker_status = {} # 각 Worker Node의 큐 상태를 저장할 딕셔너리
        self.A = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 A
        self.B = np.random.randint(1, 100, (1000, 1000))  # 1000x1000 행렬 B
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐 추가
        self.lock = threading.Lock()  # 뮤텍스 추가

    def handle_worker(self, client_socket, address):
        # Worker Node를 연결 목록에 추가 및 ID 할당
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)
        self.worker_status[worker_id] = { 'queue_used': 0, 'queue_remaining': 10 }  # 초기 큐 상태 저장
        print(f"{worker_id}연결, {address}")

        # Worker에게 ID를 명시적으로 전송
        client_socket.sendall(worker_id.encode('utf-8'))

        print("Worker Node 4개 연결, 작업 분배...")
   

    def distribute_tasks(self):
        while True:
            # 먼저 실패한 작업이 있는지 확인
            if not self.failed_queue.empty():
                with self.lock:
                    failed_task_info = self.failed_queue.get()
                    i, j = map(int, failed_task_info.split("C[")[1].rstrip(']').split(', '))
                    A_row = self.A[i, :].tolist()
                    B_col = self.B[:, j].tolist()
                    task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})

                # Worker 노드가 모두 작업 큐가 가득 찼는지 확인
               
                while self.worker_status_all_full():  # 큐가 가득 찼으면, 상태가 변할 때까지 대기
                    time.sleep(1)
                
                # 가장 적합한 Worker에게 작업 전송
                selected_worker_id = self.find_load_worker()  # 큐가 가장 여유로운 Worker 찾기
                for worker_socket, worker_id in self.worker_ids.items():
                    if worker_id == selected_worker_id:
                        worker_socket.send((task_data + "<END>").encode('utf-8'))
                        print(f"실패 작업 재전송: {worker_id} / C[{i}, {j}]")
                        break

            else:
                if not self.task_queue.empty():
                    task_data = self.task_queue.get()

                    # Worker 노드가 모두 작업 큐가 가득 찼는지 확인
                    while self.worker_status_all_full():  # 큐가 가득 찼으면, 상태가 변할 때까지 대기
                        time.sleep(0.1)

                    # 가장 적합한 Worker에게 작업 전송
                    selected_worker_id = self.find_load_worker()  # 큐가 가장 여유로운 Worker 찾기
                    for worker_socket, worker_id in self.worker_ids.items():
                        if worker_id == selected_worker_id:
                            worker_socket.send((task_data + "<END>").encode('utf-8'))
                            print(f"작업 전송: {worker_id}")
                            break

            time.sleep(1)


    def add_tasks_to_queue(self):
        # 모든 작업을 task_queue에 추가
        for i in range(1000):
            for j in range(1000):
                A_row = self.A[i, :].tolist()
                B_col = self.B[:, j].tolist()
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})
                
                #with self.lock:  # 큐에 접근할 때 뮤텍스 잠금
                self.task_queue.put(task_data)

   
            



    def find_load_worker(self):
        max_remaining = -1  # 가장 큰 queue_remaining 값을 저장할 변수
        selected_worker = None  # 선택된 Worker의 ID를 저장할 변수

        # worker_status의 모든 worker를 확인
        for worker_id, status in self.worker_status.items():
            remaining = status['queue_remaining']

            # 가장 큰 queue_remaining 값을 가진 worker를 선택
            if remaining > max_remaining:
                max_remaining = remaining
                selected_worker = worker_id
            # 동일한 queue_remaining 값이 있을 경우, Worker ID가 작은 것을 선택
            elif remaining == max_remaining and worker_id < selected_worker:
                selected_worker = worker_id

        return selected_worker  # 가장 적합한 Worker의 ID 반환


    def worker_status_all_full(self):
        # worker_status의 모든 worker의 queue_remaining 값을 확인
        for worker_id, status in self.worker_status.items():
            if status['queue_remaining'] > 0:
                return False  # 하나라도 남은 작업이 있으면 False 반환
        return True  # 모든 worker의 queue_remaining이 0이면 True 반환



    def receive_worker_status(self, client_socket, worker_id):
    # Worker Node로부터 주기적으로 큐 상태를 수신
        while True:
            try:
                status_data = client_socket.recv(1024).decode()  # Worker Node로부터 상태 수신
                if status_data:
                    status = json.loads(status_data)  # 상태 데이터를 JSON으로 디코딩
                    self.worker_status[worker_id] = {
                        'queue_used': status['queue_used'],          # 큐에 사용 중인 공간
                        'queue_remaining': status['queue_remaining']  # 큐에 남은 공간
                    }
                    # 상태 출력
                    print(f"Worker {worker_id} 상태 - 사용 중: {status['queue_used']}, 남은 공간: {status['queue_remaining']}")
            except Exception as e:
                print(f"Worker {worker_id} 상태 수신 오류: {e}")
                
   
    def receive_results(self, worker_socket):
        # 각 Worker Node로부터 결과 수신 및 재할당 처리
        try:
            while True:
                result = worker_socket.recv(1024).decode()
                if result:
                    if "failed" in result:
                        # 작업 실패 시 재할당
                        task_data = result.split("failed task for C[")[1].split(']')[0]  # C[i, j]에서 i, j만 추출
                        i, j = task_data.split(', ')
                        print(f"작업실패: {self.worker_ids[worker_socket]} / C[{i}, {j}]")
                        
                        with self.lock:  # 작업 큐에 실패한 작업을 추가할 때 뮤텍스 잠금
                            self.failed_queue.put(f"C[{i}, {j}]")  # 실패한 작업을 실패 큐에 추가
                    else:
                        # 성공한 경우 성공한 행렬 인덱스만 출력
                        task_data = result.split("C[")[1].split(']')[0]  # C[i, j]에서 i, j만 추출
                        i, j = task_data.split(', ')
                        print(f"작업성공: {self.worker_ids[worker_socket]} / C[{i}, {j}]")

                        
                time.sleep(1)  # 통신 지연 시뮬레이션
        except Exception as e:
            print(f"오류!: {self.worker_ids[worker_socket]} / {e}")


    def run(self):
        # 소켓 설정
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"Master Node 시작 {self.host}:{self.port}")

        # Worker Node의 접속을 기다림
        while self.connected_workers < 4:
            client_socket, address = server_socket.accept()
            self.handle_worker(client_socket, address)
            
            #작업 결과를 받기 위한 스레드 시작
            threading.Thread(target=self.receive_results, args=(client_socket,)).start()
            #워커의 상태를 받기 위한 스레드 시작
            threading.Thread(target=self.receive_worker_status, args=(client_socket,self.connected_workers)).start()
            


        # 작업 추가를 위한 스레드 시작
        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)
        task_addition_thread.start()

 

        # 작업 분배를 위한 스레드 시작
        distribution_thread = threading.Thread(target=self.distribute_tasks)
        distribution_thread.start()

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)  # 외부 통신을 허용하는 IP
    master_node.run()