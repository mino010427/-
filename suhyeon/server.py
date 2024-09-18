import socket
import threading
import random
import time
import logging

# 로그 설정
logging.basicConfig(filename='Master.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# 전역 상수
HOST = '0.0.0.0'  # 모든 네트워크 인터페이스에서 연결 수용
PORT = 65432
NUM_WORKERS = 4
TASK_LIMIT = 10
MATRIX_SIZE = 1000  # 행렬 크기

# 전역 변수
matrix_a = None
matrix_b = None
task_queue = []
worker_status = {}  # 작업 할당 및 대기 시간 추적

def print_and_log(message):
    """터미널과 로그 파일에 메시지 출력"""
    print(message)
    logging.info(message)

def generate_matrix():
    """랜덤 행렬 생성 함수"""
    return [[random.randint(1, 10) for _ in range(MATRIX_SIZE)] for _ in range(MATRIX_SIZE)]

def handle_worker_response(worker_id, conn):
    """워커로부터 응답 처리"""
    global matrix_a
    global matrix_b
    while True:
        try:
            # 워커로부터 응답 받기
            response = conn.recv(8192).decode()
            if not response:
                break
            message = f"워커 {worker_id}로부터 받은 응답: {response}"
            print_and_log(message)

            # 작업 데이터 업데이트
            task_data = eval(response)  # 응답 데이터 안전하게 평가
            task_result = task_data.get('result', None)
            if task_result is not None:
                print_and_log(f"워커 {worker_id}로부터 받은 결과로 행렬 업데이트")
                # 결과를 행렬에 적용 (간단한 예시)
                matrix_a[task_data['i']][task_data['j']] = task_result

            # 실패한 작업 재할당
            failed_tasks = task_data.get('failed_tasks', [])
            if failed_tasks:
                print_and_log(f"워커 {worker_id}의 실패한 작업 재할당")
                for task in failed_tasks:
                    # 작업 재할당 로직
                    pass

        except ConnectionResetError:
            print_and_log(f"연결 재설정 오류: 워커 {worker_id}가 연결을 끊었습니다")
            break
        except Exception as e:
            print_and_log(f"처리 중 오류 발생: {e}")
            break

    conn.close()

def distribute_tasks(worker_id, conn):
    """워커 노드에 작업 분배"""
    global matrix_a
    global matrix_b
    num_tasks = len(matrix_a) * len(matrix_b[0])  # 작업의 총 수
    tasks_per_worker = num_tasks // NUM_WORKERS
    remaining_tasks = num_tasks % NUM_WORKERS

    try:
        # 워커에게 작업 할당
        for i in range(NUM_WORKERS):
            task_data = {
                'matrix_a': matrix_a,
                'matrix_b': matrix_b,
                'task_range': (i * tasks_per_worker, (i + 1) * tasks_per_worker + (1 if i < remaining_tasks else 0))
            }
            conn.sendall(str(task_data).encode())
            print_and_log(f"워커 {worker_id}에게 작업 전송: {task_data}")

    except BrokenPipeError:
        print_and_log(f"파이프 오류: 워커 {worker_id}와의 연결이 끊겼습니다")
    except Exception as e:
        print_and_log(f"작업 분배 중 오류 발생: {e}")

def start_server():
    """서버 시작"""
    global matrix_a
    global matrix_b

    matrix_a = generate_matrix()
    matrix_b = generate_matrix()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()

        print_and_log(f"서버 시작: {HOST}:{PORT}, 워커 대기 중...")

        workers = {}
        for i in range(NUM_WORKERS):
            try:
                conn, addr = server_socket.accept()
                worker_id = f"워커 {i+1}"
                print_and_log(f"{addr}에서 {worker_id}와 연결됨")
                workers[worker_id] = conn

                # 워커 응답 처리 스레드 생성
                thread = threading.Thread(target=handle_worker_response, args=(worker_id, conn))
                thread.start()

            except Exception as e:
                print_and_log(f"워커 연결 오류 발생: {e}")

        # 워커에게 작업 분배
        for worker_id, conn in workers.items():
            distribute_tasks(worker_id, conn)

        # 모든 스레드가 완료될 때까지 대기
        for thread in threading.enumerate():
            if thread != threading.current_thread():
                thread.join()

if __name__ == "__main__":
    start_server()
