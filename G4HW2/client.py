import socket
import random
import numpy as np
import json
import threading
import time
import ast

HOST = '34.68.170.234' #vm 외부 ip
PORT = 8080

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST,PORT))

#client_address[0]=ip client_socket[1]=port 포트로 인덱스를 판단한다
client_address = client_socket.getsockname()


# 전역 변수를 사용하여 데이터를 전달
received_data = None #전송받은 모든 데이터를 담는 전역변수
server_count = 0 #서버로 보낸횟수
client_id = None #클라이언트 id(최초 인덱스 번호에서 정해짐)
is_row = (0, 1) #행인지 열인지 결정하는 튜플
net_round = 0 #round를 계산 후술하지만 3개의 라운드가 끝나야지 실질적으로 1라운드가 끝난다고 봐야한다
resv_rule = None #setrule을 저장할 튜플 몇번 인덱스의 클라이언트 정보를 받아야하는지 결정한다
serv_rule = () #서버로 보낼때 어떤 배열에 넣어야하는지 알려주는 튜플 
index_num = (0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9) #광훈 감사
calarr = [None,None] #계산할 배열을 저장할 전역변수

#랜덤배열 10x10 생성 함수
def generate_rannum():
    array = np.zeros((10, 10), dtype=int)
    for i in range(10):
        for j in range(10):
            array[i][j] = random.randint(1, 100)
    return array

#배열생성해서 저장
arr = generate_rannum()

# id마다 정해진 룰을따라서 데이터를 저장함
def set_rule(id):
   if id == 1:
       return [3,4,2,3,2,4]
   elif id == 2:
       return [1,4,3,4,1,3]
   elif id == 3:
       return [1,2,1,4,2,4]
   elif id == 4:
       return [2,3,1,2,1,3] 
    
#서버에 저장될 배열이 어딘지 알려주는 함수
def server_rule(id):
    if id == 1:
       return ['6','3','5']
    elif id == 2:
       return ['3','6','2']
    elif id == 3:
       return ['1','3','5']
    elif id == 4:
       return ['4','1','2'] 


#배열 2개 계산함수
def caldef(calarr):
      result = sum(x * y for x, y in zip(calarr[0], calarr[1]))
      return result


# 여기서 3번째 원소를 추출하여 id로 설정
def extract_id(data, client_port):
    if client_id != None:
        return client_id
    
    data_list = ast.literal_eval(data)
    print(data_list)
    for item in data_list:
        #print("루프확인")
        extracted_port = item[1]
        extracted_client_id = item[2]

        if extracted_port == client_port:
            return extracted_client_id

    # 일치하는 항목이 없을 경우 None 반환
    return None

#데이터 수신함수
def recv_data(client_socket):
    global received_data
    global client_id
    global serv_rule
    global server_count
    global calarr
    while True:
        data = client_socket.recv(1024)

        try:
            # 수신한 데이터 출력
            #print("Received data:", data)
            # 수신한 데이터를 JSON으로 파싱
            received_data = json.loads(data.decode())
            print("Received JSON data:", received_data)
            #print("저기왔다")
            # JSON 데이터가 정수가 아닌 경우에만 처리
            if isinstance(received_data, dict):
                id_num = received_data.get('id')
                
            #print(client_id)
            resv_rule = set_rule(client_id)
            serv_rule = server_rule(client_id)
            #print(resv_rule[net_round % 6])
            #print(id_num)

            if id_num == 5:
                #print("무시되는 데이터(서버)")
                pass
            elif resv_rule[net_round % 6] == id_num: #클라이언트 인덱스 룰과 보낸 클라이언트가 일치했을경우
                #print("받는데이터")
                round_number = received_data.get('round')
                data_content = received_data.get('data')
                is_row_flag = received_data.get('is_row')
                index_number = received_data.get('index_num')
                
                #print(id_num, round_number, data_content, is_row_flag, index_number)
                #print(calarr[0],calarr[1])
            else:
                #print("무시되는 데이터(클라)")
                pass


        except json.JSONDecodeError:
            # JSON으로 파싱할 수 없는 경우 일반 문자열로 간주
            #print("여기왔다")
            received_data = data.decode()
            print("Received plain text data:", received_data)
            #print("포트번호: ",client_address[1])#확인용
            client_id = extract_id(received_data, client_address[1])
            serv_rule = server_rule(client_id)
            print(str(received_data))

            #print(data[0])
            if data and data[0] == 123:
                temp=[str(received_data).split('}{')]
                print(temp)

            else:
                #print("아님")
                pass
            #print(client_id)#확인용

# 사용자가 입력한 데이터를 서버로 전송하는 함수
def send_data(client_socket):
    global net_round
    while True:
        while client_id is None:
            pass

        # 함수 내에서 별도의 로컬 변수로 arr을 복사
        local_arr = arr.copy()

        # 홀수 라운드인 경우
        if net_round % 2 == 1:
            data_to_send = local_arr[index_num[net_round % 20]].tolist()  # 해당 인덱스의 행을 추출하여 리스트로 변환
        # 짝수 라운드인 경우
        else:
            data_to_send = local_arr[:, index_num[net_round % 20]].tolist()  # 해당 인덱스의 열을 추출하여 리스트로 변환

       # 배열 정보 및 행/열 정보를 포함한 데이터 생성
        message = {
            'id' : client_id,
            'round': net_round, # 몇번째 전송(라운드)인지
            'data': data_to_send,  
            'is_row': is_row[net_round % 2],  # 행 또는 열 선택 홀수 라운드면 행을보내고 짝수 라운드면 열을 보냄
            'index_num': index_num[net_round % 20]
        }


        # JSON 형태로 변환하여 전송
        #아직 데이터 전송이 불완전함
        client_socket.send(json.dumps(message).encode())
        time.sleep(0.5)
        net_round = net_round + 1
        if net_round == 300: exit()
        

# 메시지 수신 쓰레드 시작
recv_thread = threading.Thread(target=recv_data, args=(client_socket,))
recv_thread.start()

# 메시지 전송 쓰레드 시작
send_thread = threading.Thread(target=send_data, args=(client_socket,))
send_thread.start()

# 메인 쓰레드는 계속 실행하여 메시지 수신 및 전송을 대기
while True:
    
    pass

client_socket.close()
