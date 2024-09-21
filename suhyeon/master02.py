import socket
import threading
import numpy as np
import time
import json
from queue import Queue

class SystemClock:
    def __init__(self):
        self.start_time = time.time()  # 시작 시간 초기화

    def get_elapsed_time(self):
        return time.time() - self.start_time  # 경과 시간 반환

class MasterNode:
    def __init__(self, host='0.0.0.0', port=9999):
        self.host = host
        self.port = port
        self.system_clock = SystemClock()  # 시스템 클락 객체 생성
        self.worker_sockets = []  # 연결된 워커 소켓 저장
        self.connected_workers = 0  # 연결된 워커 수
        self.worker_ids = {}  # 워커 ID 저장
        self.A = np.random.randint(1, 100, (1000, 1000))  # 랜덤 A 행렬 생성
        self.B = np.random.randint(1, 100, (1000, 1000))  # 랜덤 B 행렬 생성
        self.C = np.zeros((1000, 1000))  # 결과 행렬 C 초기화
        self.task_queue = Queue()  # 작업 큐
        self.failed_queue = Queue()  # 실패한 작업 큐
        self.lock = threading.Lock()  # 스레드 안전을 위한 락
        self.log_file = open("MasterLog.txt", "w")  # 로그 파일 오픈

    def handle_worker(self, client_socket, address):
        """ 워커가 연결될 때 호출되는 함수. 연결된 워커를 관리하고 초기화 함. """
        self.connected_workers += 1
        worker_id = f"worker{self.connected_workers}"  # 워커 ID 생성
        self.worker_ids[client_socket] = worker_id
        self.worker_sockets.append(client_socket)  # 소켓 리스트에 추가
        print(f"{worker_id} 연결됨: {address}")
        self.log_file.write(f"{worker_id} 연결됨: {address}\n")

        if self.connected_workers == 4:  # 모든 워커가 연결되면
            print("모든 Worker Node가 연결됨. 작업 분배 시작...")
            self.log_file.write("모든 Worker Node가 연결됨. 작업 분배 시작...\n")
            self.distribute_config()  # 작업 분배 설정 호출

    def add_tasks_to_queue(self):
        """ 행렬 곱셈 작업을 큐에 추가하는 함수. 각 C[i, j]에 대한 작업 생성. """
        for i in range(1000):
            for j in range(1000):
                A_row = self.A[i, :].tolist()  # A 행렬의 i번째 행
                B_col = self.B[:, j].tolist()  # B 행렬의 j번째 열
                task_data = json.dumps({'i': i, 'j': j, 'A_row': A_row, 'B_col': B_col})  # 작업 데이터 생성
                
                with self.lock:
                    self.task_queue.put(task_data)  # 작업 큐에 추가

    def distribute_config(self):
        """ 모든 워커에 결과 수신 스레드를 시작하고 시스템 클락을 초기화하는 함수. """
        for worker_socket in self.worker_sockets:
            threading.Thread(target=self.receive_results, args=(worker_socket,)).start()  # 결과 수신 스레드 시작

        distribution_thread = threading.Thread(target=self.distribute_tasks)  # 작업 분배 스레드 생성
        distribution_thread.start()

        self.system_clock.start_time = time.time()  # 시스템 클락 시작

    def distribute_tasks(self):
        """ 작업을 워커에게 분배하는 함수. 실패한 작업이 있을 경우 이를 우선적으로 분배. """
        while True:
            if not self.failed_queue.empty():  # 실패한 작업이 있는 경우
                with self.lock:
                    failed_task_data = self.failed_queue.get()  # 실패한 작업 가져오기
                for worker_socket in self.worker_sockets:
                    worker_socket.send(failed_task_data.encode('utf-8'))  # 실패한 작업 재전송
                    print(f"{self.worker_ids[worker_socket]}에게 실패한 작업 재전송")
                    self.log_file.write(f"{self.worker_ids[worker_socket]}에게 실패한 작업 재전송\n")
                    break
            else:
                if not self.task_queue.empty():  # 큐에 작업이 있는 경우
                    with self.lock:
                        task_data = self.task_queue.get()  # 작업 가져오기
                    for worker_socket in self.worker_sockets:
                        worker_socket.send(task_data.encode('utf-8'))  # 작업 전송
                        print(f"{self.worker_ids[worker_socket]}에게 작업 전송")
                        self.log_file.write(f"{self.worker_ids[worker_socket]}에게 작업 전송\n")
            time.sleep(1)  # 잠시 대기

    def receive_results(self, worker_socket):
        """ 워커로부터 결과를 수신하고 처리하는 함수. """
        try:
            while True:
                result = worker_socket.recv(1024).decode()  # 결과 수신
                if result:
                    if "failed" in result:  # 실패 정보가 포함된 경우
                        print(f"{self.worker_ids[worker_socket]}로부터 실패 수신: {result}")
                        self.log_file.write(f"{self.worker_ids[worker_socket]}로부터 실패 수신: {result}\n")
                        task_data = result.split("failed task for ")[1]  # 실패한 작업 데이터 추출
                        with self.lock:
                            self.failed_queue.put(task_data)  # 실패한 작업 큐에 추가
                    else:  # 성공적인 결과 수신
                        print(f"{self.worker_ids[worker_socket]}로부터 결과 수신: {result}")
                        self.log_file.write(f"{self.worker_ids[worker_socket]}로부터 결과 수신: {result}\n")
                        result_info = json.loads(result)  # 결과 정보를 JSON 형식으로 변환
                        self.C[result_info['i'], result_info['j']] = result_info['value']  # 결과 행렬 C 업데이트
                time.sleep(1)  # 잠시 대기
        except Exception as e:
            print(f"{self.worker_ids[worker_socket]}로부터 결과 수신 중 오류 발생: {e}")
            self.log_file.write(f"{self.worker_ids[worker_socket]}로부터 결과 수신 중 오류 발생: {e}\n")

    def run(self):
        """ 마스터 노드를 시작하고 워커와의 연결을 관리하는 메인 함수. """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)  # 클라이언트 연결 대기

        print(f"Master Node 시작됨: {self.host}:{self.port}")
        self.log_file.write(f"Master Node 시작됨: {self.host}:{self.port}\n")

        while self.connected_workers < 4:  # 4개의 워커가 연결될 때까지 대기
            client_socket, address = server_socket.accept()
            worker_thread = threading.Thread(target=self.handle_worker, args=(client_socket, address))  # 워커 핸들 스레드 생성
            worker_thread.start()

        task_addition_thread = threading.Thread(target=self.add_tasks_to_queue)  # 작업 추가 스레드 생성
        task_addition_thread.start()

        print("모든 Worker Node가 연결됨. 작업 분배 진행 중...")
        self.log_file.write("모든 Worker Node가 연결됨. 작업 분배 진행 중...\n")

        time.sleep(1)  # 잠시 대기
        total_time = self.system_clock.get_elapsed_time()  # 전체 소요 시간 계산
        print(f"전체 작업 소요 시간: {total_time:.2f}초")
        self.log_file.write(f"전체 작업 소요 시간: {total_time:.2f}초\n")

        self.log_file.close()  # 로그 파일 닫기

# Google Cloud VM에서 실행될 Master Node
if __name__ == "__main__":
    master_node = MasterNode(host="0.0.0.0", port=9999)
    master_node.run()
