import socket
import threading
import json
client_sockets = []
client_sockets_id = []
Need_clients = 4
idx = 1
lock = threading.Lock()

HOST = "0.0.0.0"
PORT = 8080

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen()

def send_client_sockets_id():
    for client_socket in client_sockets:
        client_socket.send(str(client_sockets_id).encode())

def send_after_recv_client(client_socket):
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        with lock:
            for other_client_socket in client_sockets:
                if other_client_socket != client_socket:
                    other_client_socket.send(data)



while True:
    client_socket, client_addr = server_socket.accept()
    print(f">>{client_addr[0]}:{client_addr[1]}에서 연결")
    client_sockets.append(client_socket)
    client_sockets_id.append((client_addr[0], client_addr[1], idx))
    idx+=1
    if len(client_sockets) == Need_clients:
        send_client_sockets_id()
        for client_socket in client_sockets:
            send_after_recv_thread = threading.Thread(target=send_after_recv_client, args=(client_socket,))
            send_after_recv_thread.start()

