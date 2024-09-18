import socket
import random
import time
import logging

# 로그 설정
logging.basicConfig(filename=f'Worker{random.randint(1,4)}.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# 전역 상수
HOST = '34.68.170.234'  # 서버 주소
PORT = 65432
TASK_LIMIT = 10

def print_and_log(message):
    """터미널과 로그 파일에 메시지 출력"""
    print(message)
    logging.info(message)

def connect_to_server():
    """서버에 연결하고 행렬 곱셈 작업 수행"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((HOST, PORT))
        print_and_log("서버에 연결됨")

        # 서버로부터 행렬 수신
        matrix_a = eval(client_socket.recv(8192).decode())
        matrix_b = eval(client_socket.recv(8192).decode())
        print_and_log("서버로부터 행렬 수신됨")

        # 작업 수행
        for task in range(TASK_LIMIT):
            # 처리 시간 시뮬레이션
            time.sleep(random.uniform(1, 3))

            # 서버에 결과 전송
            result = random.randint(1, 99)  # 결과 예시
            task_info = {'i': task, 'j': task, 'result': result}
            client_socket.sendall(str(task_info).encode())
            print_and_log(f"작업 {task}의 결과 전송: {task_info}")

            # 랜덤한 작업 실패 시뮬레이션
            if random.random() < 0.2:
                print_and_log(f"작업 {task} 실패")
                task_info['failed_tasks'] = [task]
                client_socket.sendall(str(task_info).encode())

if __name__ == "__main__":
    connect_to_server()
