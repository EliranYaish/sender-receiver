
import pika
import json
import sqlite3
import csv
import datetime
import sys
import os
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import threading



def callback(ch, method, properties, body):
    print(" [x] Received %r" % json.loads(body))
    lock = threading.Lock()
    data = json.loads(body)
    path = data['path']
    country = data['country']
    year = data['year']
    conn = DBConnection(path)
    cursor =conn.cursor()
    filePath= 'c:\\test\\'
    makeDir(filePath)
    ex4Data1 = ex1InvoicesCountryCount(cursor,filePath)
    ex2AlbumsByCountry(cursor,filePath)
    ex4Data2 = ex3BestsellerByCountryAndYear(cursor,filePath,country,year)
    ex4CreateTables(cursor)
    cur = conn.cursor()
    ex4insertData(cur,ex4Data1,ex4Data2)
    conn.commit()
    conn.close()
    



def DBConnection(path):
    try:
        conn = sqlite3.connect(path)
        return conn
    except Exception as e:
        print(e)

    return None

def makeDir(path):
    try:
        if(os.path.isdir(path)):
            print ("Directory %s Already Exist" % path)
        else:
            os.mkdir(path)
            print ("Successfully created the directory %s " % path)
    except OSError:
        print ("Creation of the directory %s failed" % path) 

def ex1InvoicesCountryCount(c,filePath):
    dataList = []
    try:
        fileName = 'Country Odrer List-'+datetime.datetime.now().strftime("%d%m%Y_%H%M%S")+'.csv'
        with open(filePath+fileName, mode='w') as countryFile:
            countryWriter = csv.writer(countryFile, delimiter=',', quotechar='"',quoting=csv.QUOTE_MINIMAL)
            data = c.execute("select BillingCountry,count(BillingCountry) from invoices group by BillingCountry")
            for row in data:
                countryWriter.writerow(row)
                dataList.append(row)
                
                

        return dataList
        print(fileName+" Created!")
        
    except Exception as e:
        print(e)


def ex2AlbumsByCountry(c,filePath):
    try:
        fileName = 'Albums List By Country-'+datetime.datetime.now().strftime("%d%m%Y_%H%M%S")+'.json'
        c.execute("select inv.billingCountry as country,group_concat(a.Title, ',') as albums from invoices inv "
                  +"left join invoice_items init on inv.invoiceId = init.InvoiceLineId "
                  +"left join tracks t on t.trackId = init.trackId "
                  +"left join albums a on a.AlbumId = t.AlbumId "
                  +"GROUP by inv.billingCountry")
        data = c.fetchall()
        jsonBuilder ={}
        jsonBuilder['AlbumsByCountry'] =[]
        for key, value in data:
            jsonBuilder['AlbumsByCountry'].append({
            'country': key,
            'albums': [value]
            })
            
        with open(filePath+fileName, mode='w') as albumsFile:
            json.dump(jsonBuilder, albumsFile)

        print(fileName+" Created!")
    except Exception as e:
        print(e)

    return None

def ex3BestsellerByCountryAndYear(c,filePath,country,year):
    try:
        fileName = 'Bestseller By Country And Year-'+datetime.datetime.now().strftime("%d%m%Y_%H%M%S")+'.xml'
        dataList = []
        c.execute("SELECT a.Title, count(t.TrackId) as bestseller from invoices inv "
                         +"LEFT JOIN invoice_items invit on inv.InvoiceId = InvoiceLineId "
                         +"LEFT JOIN tracks t on invit.TrackId = t.TrackId "
                         +"LEFT JOIN genres g on t.GenreId = g.GenreId "
                         +"LEFT JOIN albums a on t.AlbumId = a.AlbumId "
                         +"where inv.BillingCountry = '"+country+"' and inv.InvoiceDate > '"+year+"%'and g.name = 'Rock' "
                         +"group by t.AlbumId "
                         +"order by bestseller DESC "
                         +"limit 1")
        data = c.fetchone()
        top = Element('BestsellerByCountryAndYear')
        albumName = SubElement(top, 'albumName')
        albumName.text = data[0]
        dataList.append(data[0])
        countryName = SubElement(top, 'country')
        countryName.text = country
        dataList.append(country)
        sellers = SubElement(top, 'sellers')
        sellers.text = str(data[1])
        dataList.append(data[1])
        fromYear = SubElement(top, 'year')
        fromYear.text = str(year)
        dataList.append(year)

        f = open(filePath+fileName, mode='w')
        f.write(tostring(top).decode('UTF-8'))
        print(fileName+" Created!")
    except Exception as e:
        print(e)

    return dataList

def ex4CreateTables(c):
    sql_create_country_order_table = """ CREATE TABLE IF NOT EXISTS country_order (
                                    country text PRIMARY KEY,
                                    ordersCount integer  NOT NULL
                                ); """
    sql_create_bestsller_country_table = """ CREATE TABLE IF NOT EXISTS bestsller_country (
                                    albumName text,
                                    country text NOT NULL,
                                    sellerCount integer  NOT NULL,
                                    year integer  NOT NULL,
                                    PRIMARY KEY ( country, year)
                                ); """
    try:
        c.execute(sql_create_country_order_table)
        c.execute(sql_create_bestsller_country_table)
        
        print("Tables Created!")
    except Exception as e:
        print(e)

def ex4insertData(cur,data1,data2):
    sql_insert_country_order_table = """INSERT OR REPLACE INTO country_order VALUES (?, ?)"""
    sql_insert_bestsller_country_table = """INSERT OR REPLACE INTO bestsller_country VALUES (?, ?, ?, ?)"""
    try:
        cur.executemany(sql_insert_country_order_table, data1)
    except Exception as e:
        print(e)

    try:
        cur.execute(sql_insert_bestsller_country_table, data2)
    except Exception as e:
        print(e)

    print("Done!")

        
                         
        
        
    

def __init__():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='message')

    channel.basic_consume(callback,queue='message',no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

__init__()
    
