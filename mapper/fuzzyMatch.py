from fuzzywuzzy import fuzz
from util.sparkUtils import get_df_columns, get_df_columns_list, change_df_column_name

def trial_fuzzy(x,Final, Source, Destination,Final_map):
    i=Source[x]
    ignore_list=[]
    print(i)

    dest = []
    for m, n in enumerate(Destination):
        val = fuzz.ratio(i.lower(), n.lower())
        dest.append(val)
    o = 0

    for j, k in enumerate(dest):
        if Destination[j] not in Final and j not in ignore_list:
            if k >= o:
                o = k
                p = j
            else:
                pass
    print(Destination[p])

    Final[x]=Destination[p]
    Final_map[Source[x]] = o

def apply_fuzzy_wuzzy(Final, Source, Destination,Final_map):
    ignore_list = []
    PercentMatching=[]
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

        PercentMatching.append([Destination[p],p,o])
    print(PercentMatching)
    for i in PercentMatching:
        Final.append(i[0])
        Final_map[i[0]]=i[2]

    dct={}
    for i in PercentMatching:
        dct[i[1]]=dct.get(i[1],0)+1
    dct1 = {key: value for (key, value) in dct.items() if value > 1}
    print(dct1)
    for i in dct1.keys():
        print("I:",i)
        xyz=[]
        for j,k in enumerate(PercentMatching):
            print("J and K:",j,",",k)
            if i==k[1]:
                xyz.append([j,k[2]])
        print(xyz)
        xyz.sort(key=lambda x:x[1],reverse=1)
        print("SortedList ;-",xyz)
        for i in xyz[1:]:
            Final[i[0]]="Not Identified"


def re_arrange_columns(df, df1, df_dest):
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

def map_columns(spark, source_df, target_df,Column_Percentage,jobType):
    source_schema = get_df_columns(spark, source_df)
    print("source_columns:", source_schema)
    Source1 = get_df_columns_list(source_schema)
    df = source_df
    print("Source table")
    df.show()
    target_schema = get_df_columns(spark, target_df)
    print("target_columns:", target_schema)
    Destination = get_df_columns_list(target_schema)
    df_dest = target_df
    print("Destination table")
    df_dest.show()

    print("Check below matching for perticular  columns")
    print("Destination_Column", ":", "Source_Column")
    Final = []
    Final_map={}
    apply_fuzzy_wuzzy(Final, Source1, Destination,Final_map)
    for a,b in enumerate(Final):
        if b == "Not Identified":
            trial_fuzzy(a, Final, Source1, Destination,Final_map)
    print("Final MAP",Final_map)
    flag=True

    if len({i[0] for i in Final}) == len({i[1] for i in Final}):
        # df = change_df_column_name(Final, source_df)
        for key,value in Final_map.items():
            if value<Column_Percentage:
                flag=False
    if flag:
        df_auto1 = change_df_column_name(Final, source_df)
        print("Dynamically Modified Source table")
        df_auto1.show()
        return df_auto1

    else:
        print('Need user input for column mapping')

        df_auto = change_df_column_name(Final, source_df)
        print('current mapping')

        if jobType=="manual":
            rearranged_df = re_arrange_columns(source_df, df_auto, target_df)
            return rearranged_df
        else:
            print("Sending mail and aborting the job")
            pass            #Write Logic to send email
            exit(0)