import os, sqlite3
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
    
    
sendText = "주식 정보를 수집합니다."
print(sendText)

resetDB()   # 데이터베이스 초기화
getCodeList()   # 주식 목록 획득