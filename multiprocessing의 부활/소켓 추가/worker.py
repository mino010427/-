import socket
import json
import time
import random

def worker_node(master_host, master_port):
    """Worker Node를 로컬에서 실행"""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((master_host, master_port))
    print(f"Master Node와 연결 {master_host}:{master_port}")

    while True:
        task_data = client_socket.recv(1024).decode()
        if task_data:
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

            client_socket.send(result.encode('utf-8'))
            time.sleep(random.uniform(1, 2))  # 처리 시간 시뮬레이션

if __name__ == "__main__":
    master_host = '외부서버 IP 주소'  # Master Node가 있는 외부 서버 IP
    master_port = 9999
    worker_node(master_host, master_port)
