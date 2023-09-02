import telebot
import pymongo
from datetime import datetime
from pytz import timezone
import asyncio
from time import sleep
import requests
import json
from telebot import types
from pymongo import MongoClient
import sqlite3

API_KEY = '6391769840:AAEwK_31DatzAfqkDXV1mRp9ZRupzprNGDM'

bot = telebot.TeleBot(token=API_KEY,parse_mode=None)
CONNECTION_STRING = "mongodb+srv://akshay05775:jp29JILkIdDCTpu2@cluster0.8aktc9o.mongodb.net/"
client = MongoClient(CONNECTION_STRING)

def send_new_products(msg):
    db1  = client['Spam']
    spam_coll = db1['spam']
    words = spam_coll.find()
    spam = []
    for word in words:
        spam.append(word['word'].strip())
    db = client['Telegram']
    col1 = db['Products'] 
    myquery = { "New": True,"Send":False,'Active':True}
    rows = col1.find(myquery)
    for row in rows:
        prod_name = row['product_name']
        to_send = 1
        for word_ in spam:
            if word_.lower() in prod_name.lower():
                # print(f"{prod_name.lower()} contains spam word")
                to_send = 0
        if to_send:
            ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f').split()[1]         
            tele_msg = f"""Name: {row['product_name']} \n\nPrice: {row['Price']} \nM.R.P.:{row['MRP']}  \nDiscount: {row['Discount']} \nNew Product: {ind_time} \n\n{row['Link']} \n\nSeller:\n{row['Seller']}"""
            # print(tele_msg)
            msg_id = bot.send_message(msg.chat.id,tele_msg).id
            myquery = {"Product_id":row['Product_id']}
            newvalues = { "$set": { "Tele_msg_id": msg_id,"Send":True,"New":False} }
            x = col1.update_one(myquery, newvalues)

def reply_to_msg(msg,chat_id,msg_id):
    try:
        bot_token = 'bot6391769840:AAEwK_31DatzAfqkDXV1mRp9ZRupzprNGDM'
        # chat_id = '291876635'
        reply_url = f'https://api.telegram.org/{bot_token}/sendMessage?chat_id={chat_id}&text={msg}&reply_to_message_id={msg_id}'
        resp = requests.get(reply_url)
        a = json.loads(resp.text)    
        return a
    except Exception as e:
        print(e)
        pass

def send_updated_products(msg):
    db1  = client['Spam']
    spam_coll = db1['spam']
    words = spam_coll.find()
    spam = []
    for word in words:
        spam.append(word['word'].strip())
    db = client['Telegram']
    col1 = db['Products']
    myquery = { "Updated": True,"Send":False,'Active':True}
    rows = col1.find(myquery)
    for row in rows:
        prod_name = row['product_name']
        to_send = 1
        for word_ in spam:
            if word_.lower() in prod_name.lower():
                # print(f"{prod_name.lower()} contains spam word")
                to_send = 0
        if to_send:
            if row['Price']<=row['price_limit']:
                ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f').split()[1]
                rply_msg_id = row['Tele_msg_id']
                chat_id = msg.chat.id
                text = f"""Name: {row['product_name']}\n\Price: {row['Price']}\nM.R.P.: {row['MRP']} \nDiscount: {row['Discount']} \nNew Product: {ind_time} \n\n{row['Link']} \n\nSeller:\n{row['Seller']}"""
                a = reply_to_msg(text,chat_id,rply_msg_id)
                try:
                    msg_id = a['result']['message_id']
                    sql = f"UPDATE Products SET msg_id='{msg_id}',Updated=False,Send=True where id='{row[0]}'"
                    myquery = {"Product_id":row['Product_id']}
                    newvalues = { "$set": { "Tele_msg_id": msg_id,"Send":True,"Updated":False} }
                    x = col1.update_one(myquery, newvalues)
                except Exception as e:
                    print(e)

@bot.message_handler(commands=["btn"])
def send_buttons(msg):
    markup = types.ReplyKeyboardMarkup(row_width=3,one_time_keyboard=True)
    btn1 = types.KeyboardButton("Add URL")
    btn2 = types.KeyboardButton("Add spam word")
    btn3 = types.KeyboardButton("Close")
    markup.add(btn1,btn2,btn3)
    bot.send_message(chat_id=msg.chat.id,text="What you want to do?",reply_markup=markup)

@bot.message_handler(func=lambda msg:msg.reply_to_message!= None)
def reply_msgs(msg):
    db = client['Telegram']
    col1 = db['Products']
    reply_msg_id = msg.reply_to_message.message_id
    print(reply_msg_id)
    price = msg.text
    try:
        bot.reply_to(msg,f"You will get notified when the price falls below {price} for this product")
        myquery = { "Tele_msg_id": reply_msg_id }
        newvalues = { "$set": { "price_limit": int(price.strip()) } }
        x = col1.update_one(myquery, newvalues)
    except:
        pass
    
@bot.message_handler(func=lambda msg: "spam" in msg.text.lower().split()[0])
def reply_msgs(msg):
    print(f'Spam word {msg.text}')
    word = msg.text.split('-')[1]
    db = client['Spam']
    sapm_coll = db['spam']
    mylist = {'word':word}
    sapm_coll.insert_one(mylist)
    bot.reply_to(msg,f"spam word added successfully")
    
@bot.message_handler(func=lambda m:True)
def handle_msg(msg):
    BOT  = False
    if msg.text=='startbot':
        print(msg)
        BOT = True
        while BOT:
            print('Starting Now')
            send_new_products(msg)
            send_updated_products(msg)
            # sleep(2)
    
    if msg.text=='stopbot':
        BOT = False
        print('Stopping bot')
    
    if msg.text == 'Add URL':
        bot.send_message(msg.chat.id,'Enter the url')
    
    if 'http' in msg.text.split()[0]:
        url = msg.text
        db = client['Telegram']
        collection2 = db['Urls']
        # urls = collection2.find()
        mylist = {'url':url}
        collection2.insert_one(mylist)
        bot.send_message(msg.chat.id,'Url Added Successfully')
    
    

print('Starting')
bot.polling()
