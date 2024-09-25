1조 조원 구성 및 역할

20203043 권수현 - 코딩, 디버깅, git 공동 작업 생성, 과제 전반적인 스레드 관리, 코드 해석, 구글 클라우드 인스턴스 생성, 로그 관리

20203058 남태인 - 디버깅, 분배 알고리즘 설계, readme.txt 작성, git 공동 작업 생성, 과제 전반적인 스레드 관리, 코드 해석

20203072 안민호 – 과제의 기본 코드 틀 제작, readme.txt작성, git 공동작업 생성, 구글 클라우드 인스턴스 생성, 코드 해석, 발표



1. 프로그램 구성요소 : master.py - 마스터 노드, worker.py - 워커 노드

(작업 = 행렬 곱셈 연산)
◆ master.py 구성요소

- __init__ (생성자) : Master Node를 초기화(호스트, 포트설정, 시스템 클락, worker node 관리, 행렬 A, B 난수 생성, 작업 큐 및 실패 작업 큐 생성)

- run()  : 소켓 연결, Worker Node와 통신, 작업 추가, 작업 분배, 스레드 관리, 로그 관리(최종 로그 출력)

- handle_worker : 연결된 Worker Node 정보 저장, worker ID 부여, 관리

스레드 및 함수
   
   - receive_results : Worker Node가 작업(행렬 곱셈) 완료한 후 결과를 Master Node에 수신,
            성공 시 완료된 작업 저장 및 진행도 출력, 실패 시 해당 작업을 failed_queue에 넣어 Master Node가 재할당시킴

   - distribute_tasks : 작업 분배
      - worker_status_all_full : 모든 Worker Node의 queue가 가득 찼는지 확인
               작업 공간이 하나라도 남아 있을 시 False, 가득 찬 경우 True 반환

      - find_load_worker : Worker Node의 queue 상태를 기반으로 남은 queue 작업 공간이 많은 Worker Node 선택
               남은 큐 공간이 같을 경우, worker ID가 작은 Worker Node가 우선 선택

 
- add_tasks_to_queue : 곱셈 작업을 큐에 추가


◆ worker.py 구성요소

- __init__ (생성자) : Worker Node를 초기화(Master Node에 연결될 IP와 포트 저장, 시스템 클락 초기화, 작업 큐 성공/실패 카운트 설정)

- run : Master Node와 연결 설정
   - connect_to_master : Master Node에 연결하고, 연결 성공 시 Worker ID를 할당

- receive_task : Master Node로부터 작업 수신. 수신한 작업 데이터를 큐에 넣고, 큐가 가득찬 경우, 작업을 실패로 처리. 수신 성공 여부 및 worker노드의 상태 전송
- process_task : 작업 queue에서 작업을 꺼내 처리. 작업 처리시 1~3초의 시간이 소요. 작업이 80%확률로 성공, 20%확률로 실패하도록 처리. 작업 처리 결과 및 worker노드의 상태 전송



2. 소스코드 컴파일 방법 (GCP 사용)

① 구글 클라우드에 접속하여 VM instance를 생성한다.
	지역 : us-central1로 설정
	머신 유형 : e2-micro
	부팅 디스크 : Debian

② 방화벽 규칙을 추가한다
	대상 : 모든 인스턴스 선택
	소스 IP 범위 : 0.0.0.0/0  (모든 IP 주소 허용)
	프로토콜 및 포트 : TCP와 해당 포트를 지정 (port : 9999)

③ 생성된 인스턴스의 SSH를 실행한다.

④ Python과 개발 도구의 패키지들을 설치한다 (Debian 기준)
	sudo apt update
	sudo apt install python3
	sudo apt install python3-pip
	pip install numpy
	pip install numpy scipy
	pip install loguru //Python에서 로그(logging)기능을 제공하는 라이브러리

⑤ 가상환경을 생성하고 활성화한다.
	python3 -m venv myenv(가상환경 이름)
	source myenv/bin/activate //가상환경 활성화

⑥ UPLOAD FILE을 클릭하여 master.py를 업로드한다.
	master.py가 업로드된 디렉터리에서 python3 master.py로 masternode를 실행한다.

⑦ 로컬에서 powershell 터미널 4개를 열어 터미널마다 workernode를 python3 worker.py으로 실행한다.
	
	※주의할 점 
		worker.py코드 마지막 부분 master_host="외부 IP 번호"에서
		GCP에서 만든 인스턴스의 외부 IP와 같은지 확인한다.

	# Worker Node 실행
	if __name__ == "__main__":
	    worker_node = WorkerNode(master_host="34.68.170.234", master_port=9999)
	    worker_node.run()
 	
⑧ 4개의 worker node가 master node와 연결되면 프로그램이 실행된다.



3. 작업 분배 및 부하 분산에 사용한 알고리즘 설명

 distribute_tasks()는 작업 분배 함수
이는 스레드로 동작하며 task_queue와 failed_queue에 있는 작업을 분배
부하 분산을 줄이기 위해 워커4개의 작업 큐 상태를 받고 처리하는 방식을 사용

worker_status_all_full()로 4개의 워커의 작업 큐 모두 찼으면 대기를 하고 find_load_worker()로 queue remaining 값이 가장 작은 워커에게 작업을 분배

평소에는 task_queue에 있는 작업을 분배하다가 워커에서 실패한 작업이 failed_queue에 들어오면 이를 우선적으로 위의 작업 분배 방식과 동일하게 failed_queue의 작업을 분배한다.

⦁ 장점 : 모든 worker의 queue 상태를 확인 후 task를 분배하므로 균등한 작업 분배가 이루어진다.

⦁ 단점 : X



4. Error or Additional Message Handling

▶ Error Handling (Exception 처리 포함)
⊙ Master Node
	- 예외/오류 발생 시 오류 내용을 로그 파일에 기록하여 문제 추적에
	  도움을 주어 시스템 안정성을 높임

	▷ 예외 처리가 적용된 메서드 : receive_results, log_event
	⦁ receive_results
		- recv(1024)를 통해 데이터를 수신할 때, 오류 발생 시 오류 처리
		  예외 발생 시, 해당 오류 메시지를 로그에 기록

	⦁ log_event
		- 각 이벤트를 기록하는 함수로, 오류 발생 시 log_event를 통해 기록


⊙ Worker Node

	▷ 예외 처리가 적용된 메서드 : receive_task, process_task

	  ⦁ receive_task
		- task data를 수신하는 과정에서 발생할 수 있는 모든 예외를 처리,
		  예외 발생 시 해당 오류 메시지를 기록

		- task_data를 JSON으로 디코딩 중 발생하는 JSONDecodeError와 Key 		   Error를 포착
		   데이터가 잘못된 경우, 로그 파일에 오류 메시지를 기록

		- 큐가 full이면 작업 실패 메시지를 Master Node에 전송, 로그에 기록
		☆ 기대 효과: 작업 수신 중 통신 오류, 데이터 손상 등의 문제를 처리

	   ⦁ process_task
		- 작업 처리 중 실패 또는 예외 상황 발생 시 Exception 발생

		- 실패한 작업은 메시지로 출력되고, Master Node로 메시지 전송

		- 로그 파일에 작업 실패와 오류 정보를 기록
		☆ 기대 효과: 작업 처리 중 오류 발생 시 상황을 기록,
				  Master Node에게 재할당 요청할 수 있음

▶ Additional Message Handling

◇ 메시지 처리 방식
- 버퍼 관리 및 <END> 구분자를 통한 메시지 처리
	Worker Node로부터 받은 데이터를 버퍼에 저장
	<END>구분자 기준으로 메시지의 끝을 판단하여 데이터가 잘 수신되었는지 	확인 후 처리

◆ Master Node
⦁ print(log_message)
	모든 로그 출력: 각 이벤트에 대한 메시지와 경과시간 출력

⦁ self.log_event(f"{worker_id} 연결, {address}")
	워커 연결: 워커 노드가 마스터 노드에 연결될 때 출력

⦁ self.log_event("Worker Node 4개 연결 완료, 작업 분배 시작...")
	연결 완료: 워커 4개가 연결된 후 작업 분배를 시작할 때 출력

⦁ self.log_event(f"실패 작업 재전송: {worker_id} / C[{i}, {j}]")
	재전송: 실패한 작업이 다시 워커에게 전송될 때 출력

⦁ self.log_event(f"작업 전송: {worker_id}")
	작업 전송: 새로운 작업이 워커에게 전송될 때 출력

⦁ self.log_event(f"\n{self.result_matrix}")
	최종 결과 행렬 출력

⦁ self.log_event(f"오류 발생: {self.worker_ids[worker_socket]} / {e}")
	오류 발생 시 출력

⦁ self.log_event(f"작업 수신 성공: {worker_id} - 남은 큐 공간: {queue_remaining}")
	작업 수신 성공: 워커가 작업 수신 시 출력

◆ Worker Node
⦁ "Master Node와 연결 {self.master_host}:{self.master_port}"
	Worker Node가 Master Node와 연결에 성공했을 때
	Matster Node의 IP주소와 포트를 표시

⦁ "Worker ID 할당: {self.worker_id}"
       Worker Node가 Master Node로부터 고유의 Worker ID를 할당 받은 후 출력

⦁ "작업 수신 성공: {self.worker_id} / C[{i}, {j}]"
	
⦁ "작업 실패: {self.worker_id}의 큐가 가득 참 C[{i},{j}]"
	Worker Node의 작업 큐가 가득 차서 더 이상 작업을 받을 수 없을 때 출력

⦁ "작업 처리: {self.worker_id} / C[{i}, {j}]"
	Worker Node가 작업 큐에서 특정 작업을 처리할 때
	작업의 행렬 인덱스 포함 출력

⦁ "{self.worker_id} 성공: C[{i}, {j}]"
	Worker Node가 작업을 성공적으로 처리한 후 Master Node로
	성공 메시지를 전송, 로그로 출력
	- 예시) worker1 성공: C[120, 500]

⦁ "{self.worker_id} 작업 실패: C[{i}, {j}], {e}"
	Worker Node가 작업을 처리하는 도중 오류발생하여 작업 실패 시 출력
	실패 작업의 행렬 인덱스와 실패원인 e를 포함해서 출력
	- 예시) worker1 작업 실패: C[120, 580], Random failure occurred

⦁ "작업 완료 후 로그 기록 시작"
	모든 작업이 완료된 후 로그 기록 시작 시 출력

⦁ "연산 성공 횟수: {self.success_count}, 실패 횟수: {self.failure_count}"
	총 성공/실패한 작업 수를 기록할 때 출력

⦁ "작업 처리량: {total_tasks}, 평균 대기시간: {avg_waiting_time:.2f}초"
	작업 처리량과 평균 대기시간 출력

⦁ "전체 수행시간: {self.system_clock.get_elapsed_time():.2f}초"
	전체 수행시간 기록 시 출력

⦁ "작업 완료 후 로그 기록이 실행되었습니다."
	작업 완료 후 로그 기록이 실행됐음을 출력

⦁ "Master Node와 연결 {self.master_host}:{self.master_port}"
	워커가 마스터 노드와 연결됐을 때 출력



5. Additional Comments (팀플 날짜 기록)

09/14 첫 조별 모임 (도서관 스터디룸)

1. 구글 공용 계정을 생성해서 구글 클라우드에 가입 후 인스턴스를 생성   (AWS 계정 생성)
2. git으로 과제를 진행하기로 결정
   git hub desktop 설치 및 git hub 공동작업자 조원 추가
3. python 가상환경을 이용하여 설치
   python3 -m venv myenv
   source myenv/bin/activate <- 가상환경 실행 코드
4. python으로 기본적인 틀 제작
   git으로 풀, 푸시만 계속함

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

09/18
1. 균등한 작업 분배에 대한 로직 수정
2. critical section 적용 –mutex 설정 완료
3. client간 연결은 p2p방식으로 해야하는가? (고민중)
4. eval()함수 제거, json 사용
5. queue full일 때의 예외 처리

09/19 ~ 09/25
코드 수정 (불필요한 코드 제거, 오류 처리)
보고서 작성(Readme)
영상 촬영 및 제출