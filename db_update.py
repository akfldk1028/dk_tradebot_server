import sqlite3

# 데이터베이스 연결
conn = sqlite3.connect("./spot_stockdata_fastapi.db")
cursor = conn.cursor()

# 여기에서 데이터베이스 작업을 수행합니다.
# 예를 들어, 새로운 레코드 추가 또는 기존 레코드 수정 등

# 변경사항을 커밋
conn.commit()

# 연결 종료
conn.close()
