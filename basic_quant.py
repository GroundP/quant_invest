import os, sqlite3, io, json
import time, datetime, random
import pandas as pd
from selenium import webdriver # 설치필요
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import requests # 설치필요
import telepot # 설치필요
import warnings
warnings.filterwarnings('ignore')

DBPath = 'quantDB.db'
nowDateTime = datetime.datetime.now().strftime('%Y%m%d%H%M')

def resetDB():
    print("데이터베이스 초기화")
    connect = sqlite3.connect(DBPath, isolation_level=None)
    sqlite3.Connection
    cursor = connect.cursor()
    cursor.execute("DELETE FROM StockRank;")
    cursor.execute("DELETE FROM StockHaving;")
    cursor.execute("DELETE FROM QuantList;")
    connect.close()


def getCodeList():
    print("주식 목록 파일 다운로드 시작")
    dataFolder = "C:\quant\src\quant_invest\down"
    fileList = os.listdir(dataFolder)   # 파일 다운로드 전에 모든 파일 삭제
    for fileName in fileList:
        filePath = dataFolder + "\\" + fileName
        if os.path.isfile(filePath):
            os.remove(filePath)

    options = webdriver.ChromeOptions()
    #options.add_argument('headless')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option("prefs", {"download.default_directory": dataFolder})
    path = "chromedriver.exe"
    service = Service(executable_path=path)
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.get("http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101")
    time.sleep(5)   # 창이 모두 열릴 때까지 5초 기다립니다.
    driver.find_element(By.XPATH, '//*[@id="MDCSTAT015_FORM"]/div[2]/div/p[2]/button[2]/img').click()
    time.sleep(5)   # 다운로드가 될 때까지 5초 기다립니다.
    driver.find_element(By.XPATH, '//*[@id="ui-id-1"]/div/div[2]').click()
    time.sleep(5)   # 다운로드가 될 때까지 5초 기다립니다.

    fileList = os.listdir(dataFolder)
    while len(fileList) == 0:
        time.sleep(5)  # 5초 이후에도 다운로드 안되면 5초 더 기다림
        fileList = os.listdir(dataFolder)
        
    filePath = dataFolder + "\\" + fileList[0]
    driver.quit()
    
    print("파일 업로드")    # Pandas의 dataframe에 업로드
    def changeCode(code):
        code = str(code)
        code = '0' * (6-len(code)) + code   # 005930을 5930이 아닌 문자 그대로
        return code
    
    temp_df = pd.read_csv(filePath, encoding='EUC-KR')
    info_df = temp_df[['종목코드', '종목명', '종가', '시가총액', '상장주식수', '거래량', '시장구분']]
    info_df['종목코드'] = info_df['종목코드'].apply(changeCode)
    
    # 조건을 충족하지 않는 데이터를 필터링하여 새로운 변수에 저장
    noPrice = info_df['거래량'] == 0
    info_df = info_df[~noPrice]     # '~'은 틸테라고 하며 반대조건. 즉 거래량이 0인 경우 제외됨
    
    konex = info_df['시장구분'] == 'KONEX'
    info_df = info_df[~konex]   # KONEX 제외(개인은 거래가 제한, 3천만원 예수금 필요함)
    
    info_df = info_df.sort_values(by=['시가총액'])
    info_df.reset_index(drop=True, inplace=True)    # 정렬로 인한 인덱스 변경에 따른 인덱스 재설정
    
    cnt = len(info_df) * 0.2
    info_df = info_df.loc[:cnt]     # 시가총액 하위 20%만 선택
    
    print("StockList 테이블에 주식목록 업로드")
    
    connect = sqlite3.connect(DBPath, isolation_level=None)
    sqlite3.Connection
    cursor = connect.cursor()
    info_df.to_sql('TempStockList', connect, if_exists='replace')  # 임시 테이블에 기존자료 삭제 후 업로드
    sql = "INSERT INTO StockList(Code, Name, Price, MarketCap, StockIndex, NumStock, Date) SELECT 종목코드, 종목명, 종가, 시가총액, 시장구분, 상장주식수, '%s' FROM TempStockList" % (nowDateTime)
    cursor.execute(sql)
    connect.close()
    
def getCodeInfo():
    print("주식 재무정보 획득 시작") 
    
    connect = sqlite3.connect(DBPath, isolation_level=None)
    sqlite3.Connection
    cursor = connect.cursor()
    
    sql = "SELECT ID, Name, Code, NumStock, StockIndex FROM StockList WHERE Date = '%s' ORDER BY MarketCap;" % (nowDateTime)   # 오늘 날짜의 정보만 가져오기
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    cnt = 1
    for row in rows:
        print("(%s / %s) %s(%s) 주식 정보 가져오기 : " % (cnt, len(rows), row[1], row[2]))
        eps, sps, bps, cfps = 0, 0, 0, 0
        
        finance_url = 'http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&cID=&MenuYn=Y&ReportGB=B&NewMenuID=105&stkGb=701&gicode=A' + str(row[2])
        print(finance_url)
        finance_page = requests.get(finance_url, verify=False)
        time.sleep(2)
        if finance_page.text.find('error2.htm') == -1:  
            finance_text = finance_page.text.replace('(원)', '')    # 일부 주식은 (원)이 없음
            finance_tables = pd.read_html(io.StringIO(finance_text))
            temp_df = finance_tables[3]
            temp_df = temp_df.set_index(temp_df.columns[0])
        else:
            temp_df = [0]   # 일부 주식은 오류
            
        if len(temp_df) >= 23:  # 일부 주식은 CFPS, SPS가 없으므로 조회하지 않음
            temp_df = temp_df.loc[['EPS계산에 참여한 계정 펼치기', 'BPS계산에 참여한 계정 펼치기', 'CFPS계산에 참여한 계정 펼치기', 'SPS계산에 참여한 계정 펼치기']]
            temp_df.index = ['EPS', 'BPS', 'CFPS', 'SPS']
            temp_df.drop(temp_df.columns[0:4], axis=1, inplace=True)

            if str(temp_df.loc['EPS'][0]) != 'nan':
                eps = int(temp_df.loc['EPS'][0])

            if str(temp_df.loc['BPS'][0]) != 'nan':
                bps = int(temp_df.loc['BPS'][0])

            if str(temp_df.loc['CFPS'][0]) != 'nan':
                cfps = int(temp_df.loc['CFPS'][0])

            if str(temp_df.loc['SPS'][0]) != 'nan':
                sps = int(temp_df.loc['SPS'][0])

        sql_update_value = "UPDATE StockList SET EPS = %s, BPS = %s, CFPS = %s, SPS = %s, Date = '%s' WHERE ID = %s;"
        sql_update_value = sql_update_value % (eps, bps, cfps, sps, nowDateTime, row[0])
        cursor.execute(sql_update_value)
        
        if eps == 0 or bps == 0 or cfps == 0 or sps == 0:
            print("일부 기업가치 지표 미추출로 0 처리")
        else:
            print("완료")
        
        delay_time = random.uniform(1,5)
        time.sleep(random.uniform(0.5, delay_time))
        
        cnt += 1
        
    connect.close()
    print("종목별 추자정보 Insert : 완료")
    send_msg("주식 재무제표 획득 완료")
    
def send_msg(msg):
    with open('telepot.json', 'r') as file:
        data = json.load(file)
        api = data['api_key']
        chatId = data['id']
    bot = telepot.Bot(api)
    bot.sendMessage(chatId, msg)


print("주식 정보를 수집합니다.")

resetDB()   # 데이터베이스 초기화
getCodeList()   # 주식 목록 획득
getCodeInfo()   # 주식 재무제표 정보 수집