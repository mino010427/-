import socket
import threading
import random
import numpy as np
import time

# 1000x1000 행렬 생성
N = 1000
matrix_a = np.random.randint(1, 100, (N, N))
matrix_b = np.random.randint(1, 100, (N, N))
result_matrix = np.zeros((N, N))

# 소켓 설정
HOST = '0.0.0.0'  # 모든 인터페이스에서 수신
PORT = 5000

# 워커 정보
worker_sockets = []
worker_status = {}

# 장애 처리 및 재분배
def distribute_task(row, col, result, worker_address):
    """
    작업을 워커 노드로 분배
    """
    try:
        worker_sockets[worker_address].sendall(f"{row},{col}".encode())
        # 결과 대기
        data = worker_sockets[worker_address].recv(1024).decode()
        if data == "FAIL":
            # 실패하면 다른 워커에게 작업 재분배
            print(f"작업 {row}, {col} 실패 - 다른 워커로 재분배")
            assign_task(row, col)
        else:
            result_matrix[row, col] = int(data)  # 결과 업데이트
    except:
        print(f"연결 문제 발생 - {worker_address} 워커에서 재분배 중...")
        assign_task(row, col)

def assign_task(row, col):
    """
    작업을 각 워커 노드로 분배하는 메인 로직
    """
    least_loaded_worker = min(worker_status, key=worker_status.get)  # 부하가 적은 워커 선택
    distribute_task(row, col, result_matrix, least_loaded_worker)

def master_handler(conn, addr):
    """
    워커 노드와의 연결 관리
    """
    print(f"{addr}에서 연결됨")
    worker_sockets.append(conn)
    worker_status[addr] = 0  # 워커의 초기 부하 상태

    if len(worker_sockets) == 4:
        # 워커 노드가 4개 연결되었을 때만 작업을 분배 시작
        for i in range(N):
            for j in range(N):
                assign_task(i, j)

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"마스터 노드 {HOST}:{PORT}에서 대기 중...")
        
        while len(worker_sockets) < 4:
            conn, addr = s.accept()
            threading.Thread(target=master_handler, args=(conn, addr)).start()
        
        # 모든 작업이 완료될 때까지 대기
        while np.any(result_matrix == 0):  # 결과 행렬이 모두 채워질 때까지
            time.sleep(1)  # 1초마다 대기
        
        # 모든 작업이 끝나면 종료
        print("모든 행렬 연산 완료!")  # #수정: 모든 작업 완료 후 출력
        np.savetxt('result_matrix.txt', result_matrix, fmt='%d')

if __name__ == "__main__":
    main()
