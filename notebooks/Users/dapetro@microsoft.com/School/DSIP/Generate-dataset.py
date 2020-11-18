# Databricks notebook source
import pandas as pd
import random

# COMMAND ----------

# columns = ['id','amount_currency','amount_value','channel','deviceDetails_browser',
#            'deviceDetails_device','deviceDetails_deviceIp','merchantRefTransactionId','paymentMethod_apmType',
#            'paymentMethod_cardNumber','paymentMethod_cardSubType','paymentMethod_cardType','paymentMethod_cvv',
#            'paymentMethod_encodedPaymentToken','paymentMethod_expiryMonth','paymentMethod_expiryYear',
#            'shopperDetails_address_addressLine1','shopperDetails_address_addressLine2',
#            'shopperDetails_address_city','shopperDetails_address_country','shopperDetails_address_postalCode',
#            'shopperDetails_address_state','shopperDetails_email','shopperDetails_firstName','shopperDetails_lastName',
#            'shopperDetails_phoneNumber','shopperDetails_shopperKey']

columns = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

import random
def initial_values(rows):
    amounts = []
    for i in range(rows):
        amounts.append({'id':i, 'amount_value':round(random.uniform(0.00, 100000), 2)})
    return amounts

# COMMAND ----------

numbers_of_rows_in_dataset = 3000000
data = initial_values(numbers_of_rows_in_dataset)
ml_set = pd.DataFrame(data)

# COMMAND ----------

ml_set.shape

# COMMAND ----------

ml_set['amount_currency'] = 'USD'

# COMMAND ----------

def random_channel(row):
    channel = ''
    rnd =  random.randrange(101)
    if  0 < rnd <= 10:
        channel = 'pos'
    elif 10 < rnd <= 50:
        channel = 'online'
    elif 51 < rnd <= 100:
        channel = 'virtual'
    else:
        channel = 'mobile'
    
    return channel

# COMMAND ----------

ml_set['channel'] = ml_set.apply(random_channel, axis = 1)

# COMMAND ----------

ml_set.channel.unique()

# COMMAND ----------

def deviceDetails_browser(row):
    browser = ''
    rnd =  random.randrange(101)
    if  0 < rnd <= 10:
        browser = 'mozilla'
    elif 10 < rnd <= 50:
        browser = 'chrome'
    elif 51 < rnd <= 100:
        browser = 'edge'
    else:
        browser = 'chromio'
    
    return browser

# COMMAND ----------

ml_set['deviceDetails_browser'] = ml_set.apply(deviceDetails_browser, axis = 1)

# COMMAND ----------

def deviceDetails_device(row):
    device = ''
    rnd =  random.randrange(101)
    if  0 < rnd <= 10:
        device = 'mobile'
    elif 10 < rnd <= 50:
        device = 'pc'
    else:
        device = 'pos'
    
    return device

# COMMAND ----------

ml_set['deviceDetails_device'] = ml_set.apply(deviceDetails_device, axis = 1)

# COMMAND ----------

import socket
import struct
def deviceDetails_deviceIp(row):
    ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    return ip

# COMMAND ----------

ml_set['deviceDetails_deviceIp'] = ml_set.apply(deviceDetails_deviceIp, axis = 1)

# COMMAND ----------

import string
def merchantRefTransactionId(row):
    letters = string.digits
    return ''.join(random.choice(letters) for i in range(10))

# COMMAND ----------

ml_set['merchantRefTransactionId'] = ml_set.apply(merchantRefTransactionId, axis = 1)

# COMMAND ----------

def paymentMethod_apmType(row):
    amp = ''
    rnd =  random.randrange(101)
    if  0 < rnd <= 10:
        amp = 'chip'
    elif 10 < rnd <= 50:
        amp = 'magstripe '
    else:
        amp = 'nfcc'
    
    return amp

# COMMAND ----------

ml_set['paymentMethod_apmType'] = ml_set.apply(paymentMethod_apmType, axis = 1)

# COMMAND ----------

import string
def paymentMethod_cardNumber(row):
    card_number = ''
    numbers = string.digits
    for part in range(4):
        card_number += ''.join((random.choice(numbers) for i in range(4))) + '-'
    return card_number[:-1]

# COMMAND ----------

ml_set['paymentMethod_cardNumber'] = ml_set.apply(paymentMethod_cardNumber, axis = 1)

# COMMAND ----------

def paymentMethod_cardType(row):
    card_type = ''
    rnd =  random.randrange(201)
    if  0 < rnd <= 10:
        card_type = 'MasterCard'
    elif 10 < rnd <= 50:
        card_type = 'Visa '
    elif 51 < rnd <= 100:
        card_type = 'Discover '
    elif 51 < rnd <= 100:
        card_type = 'JCB'
    else:
        card_type = 'American Express'
    
    return card_type

# COMMAND ----------

ml_set['paymentMethod_cardType'] = ml_set.apply(paymentMethod_cardType, axis = 1)

# COMMAND ----------

def paymentMethod_cardSubType(row):
    card_subtype = ''
    rnd =  random.randrange(201)
    if  0 < rnd <= 10:
        card_subtype = 'Secured'
    elif 10 < rnd <= 50:
        card_subtype = 'Prepaid '
    elif 51 < rnd <= 100:
        card_subtype = 'Business '
    elif 51 < rnd <= 100:
        card_subtype = 'Student'
    else:
        card_subtype = 'Generic'
    
    return card_subtype

# COMMAND ----------

ml_set['paymentMethod_cardSubType'] = ml_set.apply(paymentMethod_cardSubType, axis = 1)

# COMMAND ----------

import string
def paymentMethod_cvv(row):
    numbers = string.digits
    return ''.join((random.choice(numbers) for i in range(3)))

# COMMAND ----------

ml_set['paymentMethod_cvv'] = ml_set.apply(paymentMethod_cvv, axis = 1)

# COMMAND ----------

def paymentMethod_encodedPaymentToken(row):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(8))

# COMMAND ----------

ml_set['paymentMethod_encodedPaymentToken'] = ml_set.apply(paymentMethod_encodedPaymentToken, axis = 1)

# COMMAND ----------

ml_set.head(5)

# COMMAND ----------

from random import randrange

def paymentMethod_expiryMonth(row):
    return randrange(12)

# COMMAND ----------

ml_set['paymentMethod_expiryMonth'] = ml_set.apply(paymentMethod_expiryMonth, axis = 1)

# COMMAND ----------

from random import randrange

def paymentMethod_expiryYear(row):
    start_year = randrange(2018,2020)
    return randrange(start_year, start_year + 10)

# COMMAND ----------

ml_set['paymentMethod_expiryYear'] = ml_set.apply(paymentMethod_expiryYear, axis = 1)

# COMMAND ----------

ml_set.head(5)

# COMMAND ----------

ml_set.to_csv('ml_datasert.csv')

# COMMAND ----------

