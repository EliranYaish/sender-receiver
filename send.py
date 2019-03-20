#!/usr/bin/env python
import pika
import json

path = input("Please enter sqlite path: ")  
print ("Path: ", path)
country = input("Please enter country: ")  
print ("Country: ", country)
year = input("Please enter year: ")  
print ("Year: ", year)
message = {'path':path, 'country':country,'year':year}
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.queue_declare(queue='message')
channel.basic_publish(exchange='',
                  routing_key='message',
                  body=json.dumps(message),
                  properties=pika.BasicProperties(
                     delivery_mode = 2, 
                  ))
print(" [x] Sent Message")
connection.close()
