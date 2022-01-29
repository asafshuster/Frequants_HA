# Frequants_HA
A short python project  the consume csv and json files contains data of users and plot it to an online graph

## installations
- sqlite 3.30.1.2
- python 3.8
- Rabbitmq
- Erlang
- pika 1.2.0 
- numpy
- sqlite3 (package)

## Structure
- **data_files:** contains the csv/json files with the users data.
- **Database:** contains the db file that the software works with.
- **files_producer.py:** the first module which reads the csv/json files and send the message whith the table name, file path, extension to the second module.
- **to_db_consumer.py:** the second module which consume the message from the first module and make IO ops to the db base on the given file type.
- **graph_consumer.py:** the third module which read the data from the db when he get a message from the second module and make a online bar plot based on the last entry. the graph shows the amount of active users and the total amount of sales per month.

## Get-Start
1. input the csv/json files with the relevent data to the data_files directory.
2. make sure that there aren't any old db files in the Database directory.
3. run files_producer.py (attention! it won't send any message if the files already sent.)
4. run to_db_consumer.py
5. run graph_consumer.py 
6. add to the data_files firectory new files and see the graph accordingly changing. 
