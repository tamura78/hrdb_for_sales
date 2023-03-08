from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd



def joblib_get_url(i):
  #iにはページ数
  url_list_pre=list()
  url=url_list_001[i]

  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html.parser")

  for i in range(0,50):
    try:
      elem=soup.find_all("div",class_="layout layoutList02")[i].find("h2").find("a")

      
      url_key=elem.attrs["href"]
      url_list_pre.append(url_key)
    except:
      pass
  return url_list_pre

def joblib_get_url_second(i):
  url_list_pre_second=list()
  url=url_list_003[i]

  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html.parser")

  try:
    elem_second=soup.find("div",class_="switch_display_btn top_type clrFix").find("ul",class_="switch_display clrFix").find_all("li")[1].find("a")
    url_key_second=elem_second.attrs["href"]
    url_list_pre_second.append(url_key_second)
  except:
     pass
  return url_list_pre_second

def joblib_get_data(i):
  new_list=list()
  #法人名 支店名 法人住所 支店住所 従業員数 業種 職種 雇用形態
  houjin=shiten=houjin_addr=shiten_addr=members=Industry=job_type=employee=" "

  url=url_list_005[i]
  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html.parser")

  #法人名
  try:
    selector="#wrapper > div.head_detail > div > div > h1"
    raw_houjin=soup.select_one(selector).get_text()
    houjin=raw_houjin.split()
  except:
    pass

  #支店名

  #法人住所　所在地
  for i in range(10):
    try:
      judge=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("th").get_text()
      if "所在地" in judge:
        raw_houjin_addr=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("td").get_text()
        houjin_addr=raw_houjin_addr.split()
    except:
      pass
  
  #支店住所　勤務地
  for i in range(10):
    try:
      judge=soup.find("table",id="job_description_table").find("tbody").find_all("tr")[i].find("th").get_text()
      if "勤務地" in judge:
        raw_shiten_addr=soup.find("table",id="job_description_table").find("tbody").find_all("tr")[i].find("td").find("p").get_text()
        shiten_addr=raw_shiten_addr.split()
    except:
      pass

  #従業員数　従業員数
  for i in range(10):
    try:
      judge=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("th").get_text()
      if "従業員数" in judge:
        raw_members=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("td").get_text()
        members=raw_members.split()
    except:
      pass

  #業種
  try:
    selector_second="#wrapper > div.bread_crumb > ul > li:nth-of-type(2) > a"#nth-child(1)はサポートされていないCSSセレクタなのでnth-of-type(1)に変換する必要がある
    raw_Industry=soup.select_one(selector_second).get_text()
    Industry=raw_Industry.split()
  except:
    pass 


  #職種　事業概要
  for i in range(10):
    try:
      judge=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("th").get_text()
      if "事業概要" in judge:
        raw_job_type=soup.find("table",id="company_profile_table").find("tbody").find_all("tr")[i].find("td").get_text()
        job_type=raw_job_type.split()
    except:
      pass  

  #雇用形態　雇用形態
  for i in range(10):
    try:
      judge=soup.find("table",id="job_description_table").find("tbody").find_all("tr")[i].find("th").get_text()
      if "雇用形態" in judge:
        raw_employee=soup.find("table",id="job_description_table").find("tbody").find_all("tr")[i].find("td").find("dl").find("dt").get_text()
        employee=raw_employee.split()
    except:
      pass 

  
  new_list.append(houjin[0])
  new_list.append(houjin_addr[0])
  new_list.append(shiten_addr[0])
  new_list.append(members[0])
  new_list.append(Industry[0])
  new_list.append(job_type[0])
  new_list.append(employee[0])
  

  return new_list


url="https://doda.jp/DodaFront/View/JobSearchList.action?sid=TopSearch&usrclk=PC_logout_kyujinSearchArea_searchButton"
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="#jobAll > div > div.boxLeft.clrFix > p > span"
total_num=soup.select_one(selector).get_text()
total_num=int(total_num.replace(",",""))

n=50 #1ページ当たりの求人数
if total_num % n ==0:
  total_page_num=total_num // n
else:
  total_page_num=total_num // n +1

url_list_001=list()
url="https://doda.jp/DodaFront/View/JobSearchList.action?sid=TopSearch&usrclk=PC_logout_kyujinSearchArea_searchButton"
url_list_001.append(url)

for i in range(2,total_page_num):
  url_key="https://doda.jp/DodaFront/View/JobSearchList.action?pic=1&ds=0&so=50&pf=0&tp=1&page="+str(i)
  url_list_001.append(url_key)


url_list_002 = list()
a=100
joblib_num = total_page_num//a + 1

for n in tqdm(range(0, joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_url)(i) for i in range(n*a,(n+1)*a) ])
        url_list_002.extend(resultList)
    except:
        pass

url_list_002_filtered=[x for x in url_list_002 if x is not None]
'''
for x in url_list_002:
  if x is not None
'''
flatten_url_list_002 = [ flatten for inner in url_list_002_filtered for flatten in inner ]

url_list_003 = list()
for i in flatten_url_list_002:
  if i not in url_list_002:
      url_list_003.append(i)


url_list_004 = list()
a=100

for n in tqdm(range(len(url_list_003))):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_url_second)(i) for i in range(n*a,(n+1)*a) ])
        url_list_004.extend(resultList)
    except:
        pass

url_list_004_filtered=[x for x in url_list_004 if x is not None]
'''
for x in url_list_004:
  if x is not None
'''
flatten_url_list_004 = [ flatten for inner in url_list_004_filtered for flatten in inner ]

url_list_005 = list()
for i in flatten_url_list_004:
  if i not in url_list_004:
      url_list_005.append(i)

b=100
joblib_num=len(url_list_004)//b+1

all_list=list()

for n in tqdm(range(0,joblib_num)):#変更点
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_data)(i) for i in range(n*b,(n+1)*b) ])
        #resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_data)(i) ])
        all_list.extend(resultList)
    except:
        pass

all_list_filtered=[x for x in all_list if x is not None]


doda_df=pd.DataFrame(all_list_filtered,columns=["法人名","法人住所","支店住所","従業員数","業種","職種","雇用形態"])
print(doda_df)

doda_df.to_csv("doda_data.csv",encoding="utf-8-sig")
