import time
import pandas as pd
import requests
import io

finance_url = 'http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&cID=&MenuYn=Y&ReportGB=B&NewMenuID=105&stkGb=701&gicode=A161570'
finance_page = requests.get(finance_url, verify=False)
time.sleep(2)
if finance_page.text.find('error2.htm') == -1:  # 일부 주식은 오류
    finance_text = finance_page.text.replace('(원)', '')    # 일부 주식은 (원)이 없음
    finance_tables = pd.read_html(io.StringIO(finance_text))
    temp_df = finance_tables[3]
    temp_df = temp_df.set_index(temp_df.columns[0])
else:
    temp_df = [0]
    
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

# if len(temp_df) >= 23:
#     temp_df = temp_df.loc[['EPS계산에 참여한 계정 펼치기', 'BPS계산에 참여한 계정 펼치기', 'CFPS계산에 참여한 계정 펼치기', 'SPS계산에 참여한 계정 펼치기']]
#     temp_df.index = ['EPS', 'BPS', 'CFPS', 'SPS']
#     temp_df.drop(temp_df.columns[0:4], axis=1, inplace=True)
    
#     if str(temp_df.loc['EPS'[0]]) != 'nan' :
#         eps = int(temp_df.loc['EPS'][0])
#     if str(temp_df.loc['BPS'[0]]) != 'nan' :
#         bps = int(temp_df.loc['BPS'][0])
#     if str(temp_df.loc['CFPS'[0]]) != 'nan' :
#         cfps = int(temp_df.loc['CFPS'][0])
#     if str(temp_df.loc['SPS'[0]]) != 'nan' :
#         sps = int(temp_df.loc['SPS'][0])
        
print(eps, bps, cfps, sps)