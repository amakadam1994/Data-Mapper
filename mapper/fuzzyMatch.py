import logging
from fuzzywuzzy import fuzz
from util.sparkUtils import get_df_columns, get_df_columns_list, change_df_column_name


def trial_fuzzy(element, final, source, destination, final_map):
    source_element = source[element]
    ignore_list = []
    dest = []
    for m, target_element in enumerate(destination):
        val = fuzz.ratio(source_element.lower(), target_element.lower())
        dest.append(val)
    index = 0
    for j, k in enumerate(dest):
        if destination[j] not in final and j not in ignore_list:
            if k >= index:
                index = k
                p = j
            else:
                pass
    print(destination[p])
    final[element] = destination[p]
    final_map[source[element]] = index


def apply_fuzzy_wuzzy(final, source, destination, final_map):
    ignore_list = []
    percent_matching = []
    for i in source:
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
    logging.info(percent_matching)
    print(percent_matching)
    for i in percent_matching:
        final.append(i[0])
        final_map[i[0]] = i[2]
    dct = {}
    for i in percent_matching:
        dct[i[1]] = dct.get(i[1], 0) + 1
    dct1 = {key: value for (key, value) in dct.items() if value > 1}
    print(dct1)
    for i in dct1.keys():
        xyz = []
        for j, k in enumerate(percent_matching):
            print("J and K:", j, ",", k)
            if i == k[1]:
                xyz.append([j, k[2]])
        xyz.sort(key=lambda x: x[1], reverse=1)
        for i in xyz[1:]:
            final[i[0]] = "Not Identified"


def re_arrange_columns(df, df1, df_dest):
    logging.info("Please check below schema for data mapping if correcct or not")
    print("Please check below schema for data mapping if correcct or not")

    Auto_source = df1.columns
    Given_source = df.columns
    for i in range(len(Auto_source)):
        logging.info(i, Given_source[i], ":", Auto_source[i])
        print(i, Given_source[i], ":", Auto_source[i])

    logging.info("Please check if all columns are mapped correctly or not ")
    print("Please check if all columns are mapped correctly or not ")

    ip = input("Y/N")
    while ip == "N":
        logging.info("Which number's column you want to change")
        print("Which number's column you want to change")
        ColumnNo = int(input())
        logging.info(df_dest.columns)
        print(df_dest.columns)
        ColumnName = input("Please give column name")
        Auto_source[ColumnNo] = ColumnName
        logging.info("Please check  below columns ")
        print("Please check  below columns ")
        for i in range(len(Auto_source)):
            print(i, Given_source[i], ":", Auto_source[i])
        ip = input("Now please check if all columns correctly mappped, Y/N")
    df_xyz = df.rdd.toDF(Auto_source)
    df_xyz.show()
    return df_xyz


def map_columns(spark, source_df, target_df, column_percentage, job_type):
    source_schema = get_df_columns(spark, source_df)
    logging.info("source_columns:", source_schema)
    print("source_columns:", source_schema)
    Source1 = get_df_columns_list(source_schema)
    df = source_df
    logging.info("Source table")
    print("Source table")
    df.show(5)

    target_schema = get_df_columns(spark, target_df)
    logging.info("target_columns:", target_schema)
    print("target_columns:", target_schema)
    destination = get_df_columns_list(target_schema)
    df_dest = target_df
    logging.info("Destination table")
    print("Destination table")
    df_dest.show(5)

    logging.info("Check below matching for perticular  columns")
    print("Check below matching for perticular  columns")
    logging.info("Destination_Column", ":", "Source_Column")
    print("Destination_Column", ":", "Source_Column")
    final = []
    final_map = {}
    apply_fuzzy_wuzzy(final, Source1, destination, final_map)
    for a, b in enumerate(final):
        if b == "Not Identified":
            trial_fuzzy(a, final, Source1, destination, final_map)
    flag = True

    if len({i[0] for i in final}) == len({i[1] for i in final}):
        for key, value in final_map.items():
            if value < column_percentage:
                flag = False
    if flag:
        df_auto1 = change_df_column_name(final, source_df)
        logging.info("Dynamically Modified Source table")
        print("Dynamically Modified Source table")
        df_auto1.show()
        return df_auto1

    else:
        logging.info('Need user input for column mapping')
        print('Need user input for column mapping')

        df_auto = change_df_column_name(final, source_df)

        if job_type == "manual":
            rearranged_df = re_arrange_columns(source_df, df_auto, target_df)
            return rearranged_df
        else:
            logging.info("Sending mail and aborting the job")
            print("Sending mail and aborting the job")
            pass  # Write Logic to send email
            exit(0)
