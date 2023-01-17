import logging
import os
from fuzzywuzzy import fuzz
from util.sparkUtils import get_df_columns, get_df_columns_list, change_df_column_name
from util.utils import get_decrypted_password


def trial_fuzzy(element_index, final, source_columns, destination, final_map):
    source_element = source_columns[element_index]
    ignore_list = []
    dest = []
    for m, target_element in enumerate(destination):
        val = fuzz.ratio(source_element.lower(), target_element.lower())
        dest.append(val)
    index = 0
    p = None
    for j, k in enumerate(dest):
        if destination[j] not in final and j not in ignore_list:
            if k >= index:
                index = k
                p = j
            else:
                pass
    if p is not None:
        final[element_index] = destination[p]
        # final_map[source_columns[element_index]] = index
        final_map[destination[p]] = index


def apply_fuzzy_wuzzy(final, source_columns, destination, final_map):
    ignore_list = []
    percent_matching = []
    for i in source_columns:
        dest = []
        for m, n in enumerate(destination):
            val = fuzz.ratio(i.lower(), n.lower())
            dest.append(val)  # j-index k-value(%)
        o = 0
        for j, k in enumerate(dest):
            if j not in ignore_list:
                if k >= o:
                    o = k
                    p = j
                else:
                    pass
        percent_matching.append([destination[p], p, o])
    logging.info(f'percent_matching:{percent_matching}')
    for i in percent_matching:
        final.append(i[0])
        final_map[i[0]] = i[2]
    dct = {}
    for i in percent_matching:
        dct[i[1]] = dct.get(i[1], 0) + 1
    dct1 = {key: value for (key, value) in dct.items() if value > 1}
    for i in dct1.keys():
        xyz = []
        for j, k in enumerate(percent_matching):
            if i == k[1]:
                xyz.append([j, k[2]])
        xyz.sort(key=lambda x: x[1], reverse=1)
        for i in xyz[1:]:
            final[i[0]] = "Not Identified"


def re_arrange_columns(source_df, auto_df, target_df):
    logging.info(f'Please check below schema for data mapping if correcct or not')

    auto_source = auto_df.columns
    given_source = source_df.columns
    for i in range(len(auto_source)):
        logging.info(i, given_source[i], ":", auto_source[i])

    logging.info(f'Please check if all columns are mapped correctly or not')
    ip = input("Y/N")
    while ip == "N":
        logging.info(f'Which numbers column you want to change')
        print("Which number's column you want to change")
        column_no = int(input())
        logging.info(f'{target_df.columns}')
        print(target_df.columns)
        column_name = input("Please give column name")
        auto_source[column_no] = column_name
        logging.info(f'Please check  below columns')
        print("Please check  below columns")
        for i in range(len(auto_source)):
            print(i, given_source[i], ":", auto_source[i])
        ip = input("Now please check if all columns correctly mappped, Y/N")
    converted_source_df = source_df.rdd.toDF(auto_source)
    converted_source_df.show()
    return converted_source_df


def map_columns(spark, source_df, target_df):
    source_schema = get_df_columns(spark, source_df)
    logging.info(f'source_columns:{source_schema}')
    source_columns = get_df_columns_list(source_schema)
    df = source_df
    logging.info(f'Source table')
    df.show(5)
    target_schema = get_df_columns(spark, target_df)
    logging.info(f'target_columns:{target_schema}')
    destination = get_df_columns_list(target_schema)
    df_dest = target_df
    logging.info(f'Target table')
    df_dest.show(5)
    logging.info(f'Check below matching for each column')
    final = []
    final_map = {}
    apply_fuzzy_wuzzy(final, source_columns, destination, final_map)
    for element_index, element in enumerate(final):
        if element == "Not Identified":
            trial_fuzzy(element_index, final, source_columns, destination, final_map)

    for i in range(len(source_columns)):
        logging.info(f'{i} {source_columns[i]} :  {final[i]} : {final_map.get(final[i])}')

    return source_columns, final, final_map