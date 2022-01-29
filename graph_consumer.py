import pika
import sqlite3
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import style
from matplotlib.animation import FuncAnimation


# Plot configurations
style.use('fivethirtyeight')
fig = plt.figure()
plt.xlabel("Months")
plt.ylabel("Amount")
plt.title("Active users and total prices per-month")


# Creates a db connection
conn = sqlite3.connect('./Database/invoice_database.db')
c = conn.cursor()

# consumer
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
finished_job_channel = connection.channel()
finished_job_channel.queue_declare(queue='finish')


def done_callback(*args):
    """Run and receive the message from the finish queue. Call plot_result()"""

    plot_results()


def get_year_month_timestamp(timestamp):
    """
    Create a timestamp object from the given timestamp string. return just the year and the month from it.
    :param timestamp: A string represent timestamp.
    :return: A string of year and month from the given timestamp

    """
    timestamp_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    year = timestamp_obj.year
    month = timestamp_obj.month
    return year, month


def get_data_from_db(table_name):
    """
    Select from the db specific columns from the given table.
    :param table_name: A given table name.
    :return: A list of entries from the db contains only customerId, InvoiceData and Total columns

    """
    data = c.execute(f"SELECT CustomerId, InvoiceDate, Total FROM {table_name}")
    return data


def count_all_active_users(sales_dct):
    """
    For each month in the sales_dct, it's count the lenth of the list of the active_users.
    :param sales_dct: A 2-D dict contains month by years and for each month a list of active users.
    :return: The given sales_dct after counting the active_users lists in each month.

    """
    for year in sales_dct:
        for month in sales_dct[year]:
            sales_dct[year][month]['active_users'] = len(sales_dct[year][month]['active_users'])
    return sales_dct


def create_dct_sales_per_month_year(db_data_obj):
    """
    Create form the db data dict as follow: {year: {month: {money: <amount>, active_users: [users_ids]}}}
    :param db_data_obj: A list of entries from the db.
    :return: A dict contains for each year, for each month the total amount of money from sales and number active_users.

    """
    sales_results_dct = {}

    for entry in db_data_obj:
        year, month = get_year_month_timestamp(entry[1])
        money = float(entry[2])/1000  # normalize the graph
        UserId = entry[0]

        if year not in sales_results_dct:
            sales_results_dct[year] = {month: {'money': money, 'active_users': [UserId]}}

        elif month not in sales_results_dct[year]:
            sales_results_dct[year][month] = {'money': money, 'active_users': [UserId]}

        elif UserId not in sales_results_dct[year][month]['active_users']:
            sales_results_dct[year][month]['money'] += money
            sales_results_dct[year][month]['active_users'].append(UserId)

        else:
            sales_results_dct[year][month]['money'] += money

    final_data_dct_to_plot = count_all_active_users(sales_results_dct)
    return final_data_dct_to_plot


def plot_results(*args):
    """Run get_data_from_db() and create_dct_sales_per_month_year(). create a bar plot that show the active users
    and the amount of sales per month corresponding to the last year which enter to the db."""

    data = get_data_from_db("t")
    results_dct = create_dct_sales_per_month_year(data)

    last_year = list(results_dct.keys())[-1]
    last_dct = results_dct[last_year]
    months = list(last_dct.keys())
    total_prices_per_month = [dct['money'] for dct in last_dct.values()]
    active_users_lst = [dct['active_users'] for dct in last_dct.values()]
    months_axis = np.arange(len(months))

    plt.cla()
    plt.bar(months_axis + 0.1, total_prices_per_month, 0.2, label='Total Prices (K)')
    plt.bar(months_axis - 0.1, active_users_lst, 0.2, label='Active users')
    plt.xticks(months_axis, months)
    plt.legend()


# Show the plot
ani = FuncAnimation(fig, plot_results, interval=1000)
plt.show()

# Consuming messages form the channel in 'finish' queue.
finished_job_channel.basic_consume(queue='finish', on_message_callback=done_callback, auto_ack=True)
print("start consuming, for exit, press CTRL C")
finished_job_channel.start_consuming()
