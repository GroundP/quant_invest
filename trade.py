import sqlite3, time, json
import telepot
from pykiwoom.kiwoom import *   # 설치필요 (PyQt5, pywin32)

TARGET_COUNT = 20

def log(str):
    print(str);
    bot.sendMessage(chatId, str)

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
    
    # PBR 순위 계산
    cursor.execute("SELECT Code, Price / BPS FROM StockList WHERE Date = '%s';" % selectDateTime)
    rows = cursor.fetchall()
    df_PBR = pd.DataFrame(rows)
    df_PBR.columns = ['Code', 'PBR']    # 컬럼명 변경
    df_PBR.set_index('Code', drop=True, append=False, inplace=True) # 인덱스를 Code로 변경
    df_PBR = df_PBR.dropna()    # 빈칸 제거
    df_PBR = df_PBR[df_PBR['PBR'] > 0]  # PBR이 0보다 큰 값만 추출
    df_PBR = df_PBR.sort_values(by='PBR')   # PBR기준 정렬 내림차순 정렬은 ascending=False를 추가
    df_PBR['rankPBR'] = df_PBR['PBR'].rank()    # 순위부여
    
    # PCR 순위 계산
    cursor.execute("SELECT Code, Price / CFPS FROM StockList WHERE Date = '%s';" % selectDateTime)
    rows = cursor.fetchall()
    df_PCR = pd.DataFrame(rows)
    df_PCR.columns = ['Code', 'PCR']    # 컬럼명 변경
    df_PCR.set_index('Code', drop=True, append=False, inplace=True) # 인덱스를 Code로 변경
    df_PCR = df_PCR.dropna()    # 빈칸 제거
    df_PCR = df_PCR[df_PCR['PCR'] > 0]  # PCR이 0보다 큰 값만 추출
    df_PCR = df_PCR.sort_values(by='PCR')   # PCR기준 정렬 내림차순 정렬은 ascending=False를 추가
    df_PCR['rankPCR'] = df_PCR['PCR'].rank()    # 순위부여

    # PER 순위 계산
    cursor.execute("SELECT Code, Price / SPS FROM StockList WHERE Date = '%s';" % selectDateTime)
    rows = cursor.fetchall()
    df_PSR = pd.DataFrame(rows)
    df_PSR.columns = ['Code', 'PSR']    # 컬럼명 변경
    df_PSR.set_index('Code', drop=True, append=False, inplace=True) # 인덱스를 Code로 변경
    df_PSR = df_PSR.dropna()    # 빈칸 제거
    df_PSR = df_PSR[df_PSR['PSR'] > 0]  # PSR이 0보다 큰 값만 추출
    df_PSR = df_PSR.sort_values(by='PSR')   # PSR기준 정렬 내림차순 정렬은 ascending=False를 추가
    df_PSR['rankPSR'] = df_PSR['PSR'].rank()    # 순위부여
    
    # 종합 순위 계산
    result_df = pd.merge(df_PER, df_PBR, how='inner', left_index=True, right_index=True)
    result_df = pd.merge(result_df, df_PCR, how='inner', left_index=True, right_index=True)
    result_df = pd.merge(result_df, df_PSR, how='inner', left_index=True, right_index=True)
        
    result_df['RankTotal'] = (result_df['rankPER'] + result_df['rankPBR'] + result_df['rankPCR'] + result_df['rankPSR']).rank()
    result_df = result_df.sort_values(by='RankTotal')
    result_df['Date'] = selectDateTime
    result_df.to_sql('StockRank', connect, if_exists='replace')
    
    print("퀀트 전략 분석 및 입력 완료")
    
def getQuantList():
    print("주식 보유 현황 DB Upate")
    result_df = kiwoom.block_request("opw00001", 계좌번호=myAccount, 비밀번호="", 비밀번호입력매체구분="00", 조회구분=3, output="예수금상세현황", next=0)
    Deposit = int(result_df['d+2추정예수금'][0])
    result_df = kiwoom.block_request("opw00018", 계좌번호=myAccount, 비밀번호="", 비밀번호입력매체구분="00", 조회구분=3, output="계좌평가결과", next=0)
    TotalPurchase = int(result_df['총평가금액'][0])
    StockCount = int(result_df['조회건수'][0])
    
    myTotalAssets = Deposit + TotalPurchase     # Equity
    Quota = int(myTotalAssets / TARGET_COUNT)     # 주식별 투입 금액
    
    log(f"총 평가잔고: {myTotalAssets}원, 주식수: {StockCount}, 주식별 투입금액: {Quota}")
    
    cursor.execute("DELETE FROM StockHaving;")
    cursor.execute("DELETE FROM QuantList;")
    
    if StockCount == 0:
        log("주식 보유 없음")
    else:
        log("주식 현황 확인")
        result_df = kiwoom.block_request("opw00018", 계좌번호=myAccount, 비밀번호="", 비밀번호입력매체구분="00", 조회구분=1, output="계좌평가잔고개별합산", next=0)
        result_df.to_sql('TempStockHaving', connect, if_exists='replace')
        cursor.execute(f"INSERT INTO StockHaving (Code, Name, HavingCount, Price, Data) SELECT replace(종목번호, 'A', ''), 종목명, 보유수량, 현재가, '{selectDateTime}' FROM TempStockHaving;")

    log("주식별 매수량, 매도량 확인")
    
    #선정된 종목에서 매수, 매도 확인
    cursor.execute(f"SELECT Code, Name FROM StockRank WHERE Date = '{selectDateTime}' ORDER BY RankTotal LIMIT {TARGET_COUNT}")
    rows = cursor.fetchall()
    
    for row in rows:
        time.sleep(1)
        Code = row[0]
        Name = row[1]
        
        result_df = kiwoom.block_request("opt10001", 종목코드=Code, output="주식기본정보", next=0)
        Price = abs(int(result_df['현재가'][0]))
        if Price == 0:
            print(f"거래정지(주가 0원)로 제외 : {result_df['종목명']}({result_df['종목코드']})")
            continue
        
        BuyingCount = Quota // Price
        
        # 보유중인 종목과 비교하여 적으면 매수, 많으면 매도
        cursor.execute(f"SELECT HavingCount FROM StockHaving WHERE Date = '{selectDateTime}' AND Code = '{Code}'")
        tempCount = cursor.fetchall()
        
        if tempCount == []:
            HavingCount = 0
        else:
            HavingCount = int(tempCount[0][0])
            
        if BuyingCount > HavingCount:
            Buy = BuyingCount - HavingCount;
            Cell = 0
        elif BuyingCount < HavingCount:
            Buy = 0
            Cell = HavingCount - BuyingCount
        else:
            Buy = 0
            Cell = 0
            
        cursor.execute("INSERT INTO QuantList VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s, '%s')" 
                       % (Code, Name, Price, Quota, BuyingCount, HavingCount, Buy, Cell, selectDateTime))
        
        # 보유중인 종목에서 List에 없는 종목 매도
        cursor.execute(f"SELECT Code, name, Price, HavingCount FROM StockHaving WHERE Date = '{selectDateTime}' AND Code NOT IN \
                       (SELECT Code From StockRank WHERE Date = '{selectDateTime}' ORDER BY RankTital LIMIT {TARGET_COUNT})")
        
        rows = cursor.fetchall()
        for row in rows:
            Code = row[0]
            Name = row[1]
            Price = row[2]
            Quota = 0
            BuyingCount = 0
            HavingCount = row[3]
            Buy = 0
            Cell = HavingCount
            Date = selectDateTime
            
            cursor.execute("INSERT INTO QuantList VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s, '%s')" 
                       % (Code, Name, Price, Quota, BuyingCount, HavingCount, Buy, Cell, Date))
            
        print("종목별 매수량, 매도량 확인 완료")

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
log("키움 로그인 완료")

# 키움 계좌 선택
tmpUserInfoAccno = kiwoom.GetLoginInfo("ACCNO")
myAccount = tmpUserInfoAccno[0]
print('선택 계좌번호 : %s' % myAccount)

# 퀀트지표별 값 및 종합 순위 결정
log('퀀트 종목을 선정합니다.')
getStockInfo(selectDateTime)

# 포트폴리오 작성
log('매매 수량을 확인합니다.')
getQuantList()  