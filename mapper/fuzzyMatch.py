from fuzzywuzzy import fuzz
from util.sparkUtils import getDfColumns, getDfColumnList

def applyFuzzyWuzzy(Final, Source, Destination):
    ignore_list = []
    for i in Source:
        dest = []
        for m, n in enumerate(Destination):
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

        if Destination[p] in Final:
            pass
        else:
            Final.append(Destination[p])


def reArrangeTheColumns(df, df1, df_dest):
    print("Please checck below schema for data mapping if correcct or not")
    Auto_source = df1.columns
    Given_source = df.columns
    for i in range(len(Auto_source)):
        print(i, Given_source[i], ":", Auto_source[i])
    print("Please check if all columns are mapped correctly or not ")
    ip = input("Y/N")
    while ip == "N":
        print("Which number's column you want to change")
        ColumnNo = int(input())
        print(df_dest.columns)
        ColumnName = input("Please give column name")
        Auto_source[ColumnNo] = ColumnName
        print("Please check  below columns ")
        for i in range(len(Auto_source)):
            print(i, Given_source[i], ":", Auto_source[i])
        ip = input("Now please check if all columns correctly mappped, Y/N")
    df_xyz = df.rdd.toDF(Auto_source)
    df_xyz.show()
    return df_xyz

def mapColumns(spark, source_df, target_df):
    source_schema = getDfColumns(spark, source_df)
    print("source_columns:", source_schema)
    Source1 = getDfColumnList(source_schema)
    df = source_df
    print("Source table")
    df.show()
    df.write.format("txt")

    target_schema = getDfColumns(spark, target_df)
    print("target_columns:", target_schema)
    Destination = getDfColumnList(target_schema)
    df_dest = target_df
    print("Destination table")
    df_dest.show()

    print("Check below matching for perticular  columns")
    print("Destination_Column", ":", "Source_Column")
    Final = []
    applyFuzzyWuzzy(Final, Source1, Destination)

    df1 = df.rdd.toDF(Final)  # Assigning schema to Source table  according to  Destination table's column
    print("Dynamically Modified Source table")
    df1.show()

    final_df = reArrangeTheColumns(df, df1, df_dest)
    return final_df
