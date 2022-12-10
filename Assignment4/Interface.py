#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    dict_to_print = []
    partition_count_dict = []
    cz = openconnection.cursor()
    range_name = "RangeRatingsPart"
    robin_name = "RoundRobinRatingsPart"

    QUERY0 = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={min} AND minrating<={max};'''
    QUERY1 = '''SELECT * FROM rangeratingspart{i} WHERE rating>={min} and rating<={max};'''
    QUERY2 = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    QUERY3 = '''SELECT * FROM roundrobinratingspart{i} WHERE rating>={min} and rating<={max};'''

    cz.execute(QUERY0.format(min=ratingMinValue, max=ratingMaxValue))

    partition_data = cz.fetchall()
    for i in partition_data:
        partition_count_dict.append(i[0])

    for partition in partition_count_dict:
        cz.execute(QUERY1.format(i=partition, min=ratingMinValue, max=ratingMaxValue))
        temp_data_3 = cz.fetchall()

        for res in temp_data_3:
            user_id = res[0]
            movie_id = res[1]
            rating = res[2]
            partition_name = range_name + str(partition)
            temp = [partition_name, user_id, movie_id, rating]
            dict_to_print.append(temp)

    cz.execute(QUERY2)
    num_rr_parts = cz.fetchall()[0][0]

    for i in range(0, num_rr_parts):
        cz.execute(QUERY3.format(i=i, min=ratingMinValue, max=ratingMaxValue))
        temp_res_1 = cz.fetchall()
        for res in temp_res_1:
            user_id = res[0]
            movie_id = res[1]
            rating = res[2]
            partition_name = robin_name + str(i)
            temp = [partition_name, user_id, movie_id, rating]
            dict_to_print.append(temp)

    writeToFile('RangeQueryOut.txt', dict_to_print)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    dict_to_print = []
    partition_count_dict = []
    cz = openconnection.cursor()
    range_name = "RangeRatingsPart"
    robin_name = "RoundRobinRatingsPart"
    QUERY0 = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={target} AND minrating<={target};'''
    QUERY1 = '''SELECT * FROM rangeratingspart{i} WHERE rating={target};'''
    QUERY2 = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    QUERY3 = '''SELECT * FROM roundrobinratingspart{i} WHERE rating={target};'''

    cz.execute(QUERY0.format(target=ratingValue))
    temp_data1 = cz.fetchall()
    for item1 in temp_data1:
        partition_count_dict.append(item1[0])

    for partition in partition_count_dict:
        cz.execute(QUERY1.format(i=partition, target=ratingValue))
        temp_data2 = cz.fetchall()
        for res in temp_data2:
            user_id = res[0]
            movie_id = res[1]
            rating = res[2]
            partition_name = range_name + str(partition)
            temp = [partition_name, user_id, movie_id, rating]
            dict_to_print.append(temp)

    cz.execute(QUERY2)
    count_of_parts = cz.fetchall()[0][0]

    for i in xrange(0, count_of_parts):
        cz.execute(QUERY3.format(i=i, target=ratingValue))
        temp_data_3 = cz.fetchall()
        for res in temp_data_3:
            user_id = res[0]
            movie_id = res[1]
            rating = res[2]
            partition_name = robin_name + str(i)
            temp = [partition_name, user_id, movie_id, rating]
            dict_to_print.append(temp)

    writeToFile('PointQueryOut.txt', dict_to_print)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
