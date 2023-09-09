import pymongo
import requests
import bs4
from pymongo import MongoClient
from threading import Thread
from time import sleep
import gspread
import pandas as pd

# Google sheet
API_key = '18i2CHgGRESZaTlWi50tnVFDZYn2dL0a2fcny8tWkh-I'
gc = gspread.service_account(filename='google.json')
worksheet = gc.open_by_key(API_key)
current_sheet = worksheet.worksheet('Products')
try:
    shoes = worksheet.add_worksheet('shoes',10000,20)
except:
    pass

# MongoDB
CONNECTION_STRING = "mongodb+srv://akshay05775:jp29JILkIdDCTpu2@cluster0.8aktc9o.mongodb.net/"
client = MongoClient(CONNECTION_STRING)
db = client['Telegram']
collection2 = db['Urls']
col1 = db['Products']

def list_to_string(list):
    """Converts a list to a string."""
    items = []
    for item in list:
        items.append(str(item))
    return items

def get_products(url):
    db = client['Telegram']
    link = 'https://www.flipkart.com'
    data = requests.get(url)
    soup = bs4.BeautifulSoup(data.content,'html.parser')
    link2 = "&sort=price_asc&affid=rohanpouri&affExtParam1=ENKR20230807A555130641&affExtParam2=ENKR20230807A555130641"
    products = soup.find_all('div','_4ddWXP')
    df = pd.DataFrame()
    if products:
        for i in products[:]:
            try:
                name = i.find('a','s1Q9rs').text
                l = link+i.find('a','s1Q9rs').get('href')
                l1 = l[l.index('pid'):]
                pid = l1[:l1.index("&")]
                prod_id = pid.split('=')[1].strip()
                sellers_link = "https://www.flipkart.com/sellers?" + pid + link2
                price = []
                for j in i.find('div','_30jeq3').text:
                    if j.isnumeric():
                        price.append(j)
                price_ = int("".join(price))
                MRP = []
                for j in i.find('div','_3I9_wc').text:
                    if j.isnumeric():
                        MRP.append(j)
                MRP_ = int("".join(MRP))
                Discount = i.find('div','_3Ay6Sb').text.split()[0]
                coll = db['Products']
                myquery = { "Product_id":prod_id}
                prod = col1.find_one(myquery)
                if prod:
                    if prod['Price'] == price_:
                        # print('Passing')
                        pass
                    else:
                        # print('Updating')
                        myquery = { "Product_id": prod_id}
                        newvalues = { "$set": { "Updated": True,"Send":False,"New":False,"Price":price_,"MRP":MRP_,
                                               "Discount":Discount,"Link":l,'users':''} }
                        x = col1.update_one(myquery, newvalues)

                else:
                    # print('Inserting new product')
                    
                    mylist = {"product_name":name,"Link":l,"Product_id":prod_id,"Seller":sellers_link,"Price":price_,"MRP":MRP_,
                              "Discount":Discount,"price_limit":0,"Updated":False,"New":True,"Send":False,'Active':True,"Tele_msg_id":0,'Blocked':False,'users':''}
                    # print(mylist)
                    df1 = pd.DataFrame([mylist])
                    df = pd.concat([df,df1],ignore_index=True)
                    col1.insert_one(mylist)
            except Exception as e:
                # print(e)
                pass
    else:
        df = pd.DataFrame()
        products = soup.find_all('div','_2kHMtA')
        for i in products[:]:
            try:
                name = i.find('div','_4rR01T').text
                l = link + i.find('a').get('href')
                l1 = l[l.index('pid'):]
                pid = l1[:l1.index("&")]
                prod_id = pid.split('=')[1].strip()
                sellers_link = "https://www.flipkart.com/sellers?" + pid + link2
                price = []
                for j in i.find('div','_30jeq3 _1_WHN1').text:
                    if j.isnumeric():
                        price.append(j)
                price_ = int("".join(price))
                MRP = []
                for j in i.find('div','_3I9_wc _27UcVY').text:
                    if j.isnumeric():
                        MRP.append(j)
                MRP_ = int("".join(MRP))
                Discount = i.find('div','_3Ay6Sb').text
                coll = db['Products']
                myquery = { "Product_id":prod_id}
                prod = col1.find_one(myquery)
                if prod:
                    if prod['Price'] == price_:
                        # print('Passing')
                        pass
                    else:
                        # print('Updating')
                        myquery = { "Product_id": prod_id}
                        newvalues = { "$set": { "Updated": True,"Send":False,"New":False,"Price":price_,"MRP":MRP_,
                                               "Discount":Discount,"Link":l,'users':''} }
                        x = col1.update_one(myquery, newvalues)

                else:
                    # print('Inserting new product')
                    mylist = {"product_name":name,"Link":l,"Product_id":prod_id,"Seller":sellers_link,"Price":price_,"MRP":MRP_,
                              "Discount":Discount,"price_limit":0,"Updated":False,"New":True,"Send":False,'Active':True,"Tele_msg_id":0,'Blocked':False,'users':''}
#                     print(mylist)
                    df1 = pd.DataFrame([mylist])
                    df = pd.concat([df,df1],ignore_index=True)
                    col1.insert_one(mylist)
            except Exception as d:
                # print(d)
                pass
            
    if df.empty:
        pass
    else:
        x = []
        for row in list(df[['product_name', 'Link', 'Product_id', 'Seller', 'Price', 'MRP','Discount', 'price_limit','Blocked']].values):
            x.append(list_to_string(row))
        current_sheet.append_rows(x)

while True:
    db = client['Telegram']
    collection2 = db['Urls']
    urls = collection2.find()
    ur = []
    for i in urls:
        ur.append(i['url'])
    
    a = len(ur)
    print(a)
    # for i in range(a):
    threads = []
    for i in range(a):
        if i:
            t = Thread(target=get_products,args=(ur[i],))
            t.start()
            threads.append(t)

    for idx,k in enumerate(threads):
        k.join()
    sleep(1)
