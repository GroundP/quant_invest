import sqlite3, time, json
import telepot
from pykiwoom.kiwoom import *   # 설치필요 (PyQt5, pywin32)

# 함수 선언
def getStockInfo(selectDateTime):
    # PER 순위 계산
    cursor.execute("SELECT Code, Name, Price / EPS FROM StockList WHERE Date = '%s';" % selectDateTime)
    rows = cursor.fetchall()
    df_PER = pd.DataFrame(rows)
    df_PER.columns = ['Code', 'Name', 'PER']    # 컬럼명 변경
    df_PER.set_index('Code', drop=True, append=False, inplace=True) # 인덱스를 Code로 변경
    df_PER = df_PER.dropna()    # 빈칸 제거
    df_PER = df_PER[df_PER['PER'] > 0]  # PER이 0보다 큰 값만 추출
    df_PER = df_PER.sort_values(by='PER')   # PER기준 정렬 내림차순 정렬은 ascending=False를 추가
    df_PER['rankPER'] = df_PER['PER'].rank()    # 순위부여
    print(df_PER)
    

# 텔레그램 메세지 전송 설정
with open('telepot.json') as file:
    data = json.load(file)
    api = data['api_key']
    chatId = data['id']

bot = telepot.Bot(api)

# 주식 정보 선택
DBPath = 'quantDB.db'
connect = sqlite3.connect(DBPath, isolation_level=None)
sqlite3.Connection
cursor = connect.cursor()

# 가장 최근 재무제표 Date 조회
sql = "SELECT MAX(Date) FROM StockList;"
cursor.execute(sql)
dateList = cursor.fetchall()
selectDateTime = dateList[0][0]

# 키움 로그인
kiwoom = Kiwoom()   # 키움 API사용
kiwoom.CommConnect(block=True)  # 블로킹 처리(로그인 완료까지 대기)
print("키움 로그인 완료")
bot.sendMessage(chatId, "키움 로그인 완료")

# 키움 계좌 선택
tmpUserInfoAccno = kiwoom.GetLoginInfo("ACCNO")
myAccount = tmpUserInfoAccno[0]
print('선택 계좌번호 : %s' % myAccount)

# 퀀트지표별 값 및 종합 순위 결정
print('퀀트 종목을 선정합니다.')
bot.sendMessage(chatId, '퀀트 종목을 선정합니다.')
getStockInfo(selectDateTime)