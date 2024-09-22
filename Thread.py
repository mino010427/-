import socket
import threading
from queue import Queue

# 스레드별 큐 생성 (thread1, thread2 용 큐)
thread1_queue = Queue()
thread2_queue = Queue()

def handle_thread1():
    while True:
        data = thread1_queue.get()
        print(f"Thread 1 received data: {data}")

def handle_thread2():
    while True:
        data = thread2_queue.get()
        print(f"Thread 2 received data: {data}")

def handle_client(client_socket):
    while True:
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if data.startswith("thread1:"):
                thread1_queue.put(data.split(":")[1])  # thread1이 처리할 데이터를 큐에 넣음
            elif data.startswith("thread2:"):
                thread2_queue.put(data.split(":")[1])  # thread2가 처리할 데이터를 큐에 넣음
        except Exception as e:
            print(f"Error: {e}")
            break

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', 9999))  # 모든 IP에서 연결 허용
    server_socket.listen(5)
    print("Server listening on port 9999...")

    # 클라이언트와 연결
    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")

    # 스레드 시작 (thread1, thread2)
    thread1 = threading.Thread(target=handle_thread1)
    thread2 = threading.Thread(target=handle_thread2)
    thread1.start()
    thread2.start()

    # 클라이언트에서 온 데이터를 처리하는 스레드 시작
    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()

if __name__ == "__main__":
    main()
