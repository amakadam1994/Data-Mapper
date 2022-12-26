def getCommonJars(parent_path, source, target, config):
    jars = []
    source_jars_comma = config[source]['jars']
    print("source_jars_comma:", source_jars_comma)
    if source_jars_comma == None or source_jars_comma == "None":
        pass
    else:
        source_jars_list = source_jars_comma.split(",")
        for jar in source_jars_list:
            jar_path = parent_path + jar
            if jars.__contains__(jar_path):
                pass
            else:
                jars.append(jar_path)

    target_jars_comma = config[target]['jars']
    if target_jars_comma == None or target_jars_comma == "None":
        pass
    else:
        target_jars_list = target_jars_comma.split(",")
        for jar in target_jars_list:
            jar_path = parent_path + jar
            if jars.__contains__(jar_path):
                pass
            else:
                jars.append(jar_path)

    return ','.join(map(str, jars))
