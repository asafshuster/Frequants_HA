# Imports
import json
import pika
import sqlite3
import csv


# Connect to the db
conn = sqlite3.connect('./Database/invoice_database.db')
c = conn.cursor()

# create message channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='files')


def done_message():
    """Send a 'done' message to 'finish' queue"""

    channel.basic_publish(exchange='', routing_key='finish', body=bytes("done".encode('utf-8')))
    print("Done message was sent")


def format_message(body_message):
    """
    Decode the message and split it to table name, file path and extension.
    :param body_message: The body of the message from 'files' queue contains table name, file path, extension.
    :return: table name, file path and extension
    :rtype: string

    """
    table_name, file_path, extension = body_message.decode('utf-8').split(', ')
    return table_name, file_path, extension


def ops_funcs_by_ext(table_name, file_path, file_ext):
    """
    Call the json/ csv by the file extension.
    :param table_name: A name of a table sent from a message from 'file' queue.
    :param file_path: A file path sent from a message from 'file' queue.
    :param file_ext: A file extension corresponding to the file path sent from a message from 'file' queue.
    :return: None

    """
    if file_ext == ".json":
        json_writer_to_database(table_name, file_path)
    else:
        csv_writer_to_database(table_name, file_path)


def json_writer_to_database(table_name, file_path):
    """
    Read the given json file, create list of tuples contains the values from the json.
    Insert each tuple as entry to the db (to the given table name).

    :param table_name: A given table name.
    :param file_path: A given file path.
    :return: None

    """
    with open(file_path, "r") as json_f:
        to_db = [(i['InvoiceId'], i['CustomerId'], i['InvoiceDate'], i['BillingAddress'],
                  i['BillingCity'], i['BillingState'], i['BillingCountry'], i['BillingPostalCode'],
                  i['Total']) for i in json.load(json_f)]
        commend = f"""INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        c.executemany(commend, to_db)
        conn.commit()


def csv_writer_to_database(table_name, file_path):
    """
    Read the given file, create list of tuples contains the values from the json.
    Insert each tuple as entry to the db (to the given table name).

    :param table_name: A given table name
    :param file_path: A given file path
    :return: None

    """
    with open(file_path, 'r') as csv_f:
        data = csv.DictReader(csv_f)
      
        to_db = [(i['InvoiceId'], i['CustomerId'], i['InvoiceDate'], i['BillingAddress'],
              i['BillingCity'], i['BillingState'], i['BillingCountry'], i['BillingPostalCode'],
              i['Total']) for i in data]
        commend = f"""INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""

        c.executemany(commend, to_db)
        conn.commit()


def create_table(table_name):
    """
    Create a new table for the data form the files in data_files dir.
    :param table_name: A given table name
    :return: None

    """
    commend = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    InvoiceId, CustomerId, InvoiceDate, BillingAddress,
                    BillingCity, BillingState, BillingCountry, BillingPostalCode,
                    Total);"""
    c.execute(commend)

        
def callback(*args):
    """
    Call format_message(), create_table(), ops_func_by_ext() and done_message()
    :param args: properties of the message sent from the producer.
    :return: None

    """
    body = args[3]
    print("Message received")
    table_name, file_path, extension = format_message(body)
    create_table(table_name)
    ops_funcs_by_ext(table_name, file_path, extension)
    done_message()


channel.basic_consume(queue='files', on_message_callback=callback, auto_ack=True)
print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()

