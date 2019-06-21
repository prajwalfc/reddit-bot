import subprocess

table_list = ["2015_01","2015_02","2015_03","2015_04","2015_05","2015_06","2015_07","2015_08","2015_09","2015_10","2015_11","2015_12","2016_01","2016_02","2016_03","2016_04","2016_05","2016_06","2016_07","2016_08","2016_09","2016_10","2016_11","2016_12","2017_01","2017_02","2017_03","2017_04","2017_05","2017_06","2017_07","2017_08","2017_09","2017_10","2017_11","2017_12","2018_01","2018_02","2018_03","2018_04","2018_05","2018_06","2018_07","2018_08","2018_09","2018_10","2018_11","2018_12","2019_01","2019_02","2019_03","2019_04"]

for table in table_list[0:1]:
    command = 'bq extract --destination_format NEWLINE_DELIMITED_JSON fh-bigquery:reddit_comments.{} gs://newprajwalfc/reddit/reddit_{}_*.json'.format(table,table)
    #print(command)
    subprocess.Popen(command,shell=True, stdin=None,stdout=None,stderr=None,close_fds=True)