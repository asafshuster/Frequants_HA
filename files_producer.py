# Imports
import pika
from os import getcwd, listdir
from os.path import isfile, join, splitext
import sqlite3

# Constants
DATA_FILES_DIR = getcwd() + "/data_files"
TABLE_NAME = "t"
USED_FILE_TABLE_N = "Used_Files"

# create connection to the db
conn = sqlite3.connect('./Database/invoice_database.db')
c = conn.cursor()


# Create Connection
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='files')


def create_used_files_table():
    """Create a table of used files in the db"""

    commend = f"""CREATE TABLE IF NOT EXISTS {USED_FILE_TABLE_N} (paths);"""
    c.execute(commend)
    conn.commit()


def insert_used_file_to_table(file_path):
    """
    Insert file path of used file to the db
    :param file_path: A given file path
    :return: None

    """
    commend = f"""INSERT INTO {USED_FILE_TABLE_N} (paths)
                  VALUES ('{file_path}');"""
    c.execute(commend)
    conn.commit()


def get_used_file_from_db():
    """
    Select all the paths under 'Used_files' table from the db
    :return: A list of used files

    """
    commend = f"""SELECT paths FROM {USED_FILE_TABLE_N}"""
    query = c.execute(commend)
    paths_lst = [tpl[0] for tpl in query]
    return paths_lst


def get_files_paths(used_files_lst):
    """
    Create a generetor object contains tuple of (file path, extension) based on the files from the data_files
    in the project directory if the files are not used yet and are '.json' or '.csv'.

    :param used_files_lst: a list of all the used files retrieved from the 'Used_files' table from the db.
    :return: A generator object build from tuples of '(file path, extension)'

    """
    files_gen = ((f"{DATA_FILES_DIR}/{file}", splitext(file)[1]) for file in listdir(DATA_FILES_DIR)
                 if isfile(join(DATA_FILES_DIR, file))
                 and splitext(file)[1] in ['.json', '.csv']
                 and f"{DATA_FILES_DIR}/{file}" not in used_files_lst)  # to avoid duplicated data in the db.
    return files_gen


def send_data_to_queue(files_gen):
    """
    generate files paths, extensions from the generator, create a message contains table name, file path.
    Send the message to 'files' queue consumer. Insert the current file path in the iteration of the generator to the db

    :param files_gen: A generator object contains tuples of (file paths, extension)
    :return: None

    """
    for file in files_gen:
        file_path = file[0]
        file_ext = file[1]
        message = f"{TABLE_NAME}, {file_path}, {file_ext}".encode('utf-8')
        channel.basic_publish(exchange='', routing_key='files', body=bytes(message))
        insert_used_file_to_table(file_path)
        print("Message was sent")


def main():
    create_used_files_table()
    used_files_lst = get_used_file_from_db()
    files_gen = get_files_paths(used_files_lst)
    send_data_to_queue(files_gen)
    channel.close()


if __name__ == '__main__':
    main()