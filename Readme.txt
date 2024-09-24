1조 조원 역할

20203043 권수현 - 코딩

20203058 남태인 - 코딩

20203072 안민호 –과제의 기본 코드 틀 제작, readme.txt작성, git 공동작업 생성, 구글 클라우드 인스턴스 생성, queue_status 작업

1. 프로그램 구성요소 :

<master.py 구성요소>

- __init__ (생성자) : Master Node를 초기화(호스트, 포트설정, 시스템 클락, worker node 관리, 행렬 A, B 난수 생성, 작업 큐 및 실패 작업 큐 생성)
- handle_worker : Worker Node와 연결 처리, worker ID 부여, 상태 관리
- distribute_tasks : 
- add_tasks_to_queue : 행렬 A와 B의 모든 요소에 대한 곱셈 작업을 큐에 추가
- find_load_worker : Worker Node의 queue 상태를 기반으로 가장 적합한 Worker Node를 찾음. queue 남은 작업 공간(queue_remaining)이 많은 Worker Node 선택Worker Node의 queue_remaining이 같을 경우, worker ID가 작은 Worker Node가 우선 선택
- worker_status_all_full : 모든 Worker Node의 queue가 가득 찼는지 확인, 작업 공간이 하나라도 남아 있을 시 False, 가득 찬 경우 True 반환
- receive_worker_status : Worker Node로부터 주기적으로 Worker Node의 상태 정보를 수신 및 업데이트Worker Node의 queue_remaining과 queue_used (사용 중인 queue)를 업데이트
- receive_results : Worker Node가 작업(행렬 곱셈) 완료한 후 결과를 Master Node에 수신하고, 성공 여부를 판단성공 시 완료된 작업 기록, 실패 시 해당 작업을 failed_queue에 넣어 Master Node가 재할당할 수 있게 함
- run : Master Node의 메인 실행 메서드, Worker Node와 통신, 작업 추가, 분배 등을 각각의 스레드에서 동시 처리하도록 함

<worker.py 구성요소>

- __init__ (생성자) : Worker Node를 초기화(Master Node에 연결될 IP와 포트 저장, 시스템 클락 초기화, 작업 큐 성공/실패 카운트 설정)
- connect_to_master : Master Node에 연결하고, 연결 성공 시 Worker ID를 할당
- report_queue_status : 현재 사용 중인 큐 크기와 남은 큐 공간을 Master Node에 보고. 작업 성공/실패할 때에도 큐의 상태를 알려줌
- receive_task : Master Node로부터 작업 수신. 수신한 작업 데이터를 큐에 넣고, 큐가 가득찬 경우, 작업을 실패로 처리. 실패 메시지는 Master Node로 전송
- process_task : 작업 queue에서 작업을 꺼내 실제로 처리. 작업 처리시 1~3초의 시간이 소요. 작업이 80%확률로 성공, 20%확률로 실패하도록 처리
- run : Worker Node를 실행, Master Node와 연결 설정 후, 작업 수신과 작업 처리를 각가가 별도 스레드에서 동시 수행함


2. 소스코드 컴바일방법

- 구글 클라우드를 실행한다. SSH를 실행한 후 UPLOAD FILE을 하여 masternode.py를 업로드한다.
- python3 masternode.py를 하여 masternode를 실행한다.
- powershell을 통해 workernode를 python3 workernode.py를 실행한다.
  (단, 4개의 터미널을 열어서 workernode를 실행해야 한다.)


3. 작업 분배 및 부하 분산에 사용한 알고리즘 설명

장점 :
단점 :

4. Error or Additional Message Handling




5. Additional Comments (팀플 날짜 기록)

2024-09-14
1. 구글 공용 계정을 생성해서 구글 클라우드에 가입 후 인스턴스를 생성   (AWS 계정 생성)
2. git으로 과제를 진행하기로 결정
   git hub desktop 설치 및 git hub 공동작업자 조원 추가
3. python 가상환경을 이용하여 설치
   python3 -m venv myenv
   source myenv/bin/activate <- 가상환경 실행 코드
4. python으로 기본적인 틀 제작
코드에서 문제점 발견 (eval()의 문제점)
예외가 발생했습니다. SyntaxError
invalid syntax (<string>, line 1)
  File "C:\Users\USER\OneDrive\바탕 화면\데이터통신\multithreads\queue추가\workernode.py", line 109, in process_task
    i, j, A_row, B_col = eval(task_data)
                         ^^^^^^^^^^^^^^^
SyntaxError: invalid syntax (<string>, line 1)
이런 예외처리를 위해서 json 문자열로 변환하였다.
json방식을 사용하여 데이터를 인코딩/디코딩하는 것이 더 안정되고 권장하는 방식

eval() 함수의 문제 (보안문제, 문자열을 보다 안전하게 주고 받을 수 있는 json사용 권장)

2024-09-18
1. 균등한 작업 분배에 대한 로직 수정
2. critical section 적용 –mutex 설정 완료
3. client간 연결은 p2p방식으로 해야하는가? (고민중)
4. eval()함수 제거, json 사용
5. queue full일 때의 예외 처리



