import socket
import random
import time
import numpy as np

HOST = '34.68.170.234'  # 마스터 노드의 GCP 공인 IP 주소
PORT = 5000

def matrix_multiply(row, col, matrix_a, matrix_b):
    """
    행렬 곱 연산
    """
    N = len(matrix_a)
    return sum(matrix_a[row][k] * matrix_b[k][col] for k in range(N))

def worker():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        matrix_a = np.random.randint(1, 100, (1000, 1000))  # 임시 행렬 생성
        matrix_b = np.random.randint(1, 100, (1000, 1000))

        while True:
            data = s.recv(1024).decode()
            if not data:
                break

            row, col = map(int, data.split(','))
            processing_time = random.uniform(1, 3)
            time.sleep(processing_time)  # 가상 처리 시간

            # 작업 성공/실패 80% 확률
            if random.random() <= 0.8:
                result = matrix_multiply(row, col, matrix_a, matrix_b)
                s.sendall(str(result).encode())
            else:
                s.sendall("FAIL".encode())

if __name__ == "__main__":
    worker()
