import numpy as np
import pandas as pd
import pymysql



# Import CSV

data = pd.read_csv(r'./doc/tweets_s.csv')
df = pd.DataFrame(data, columns=['id','userid_str', 'screen_name','created_at','source','in_reply_to_status_id_str','in_reply_to_screen_name', 'retweet_count','favorite_count','text'])
df = df.replace({np.nan: None})
# id,userid_str,screen_name,created_at,source,in_reply_to_status_id_str,in_reply_to_screen_name,retweet_count,favorite_count,text

# Connect to SQL Server

conn = pymysql.connect(host="localhost", user="root", passwd="", database="499_s")
cursor = conn.cursor()


# Create Table

cursor.execute('DROP TABLE IF EXISTS `zctweets_s`;')
cursor.execute('CREATE TABLE `499_s`.`zctweets_s` ( `id` BIGINT(20) , `userid_str` BIGINT(20) NOT NULL, `screen_name` TEXT ,`created_at` TIMESTAMP  ,`source` TEXT , `in_reply_to_status_id_str` BIGINT(20), `in_reply_to_screen_name` TEXT, `retweet_count` INT, `favorite_count` INT , `text` TEXT '
               ',Primary Key(id)) ENGINE = InnoDB;'

               )


# f = open("./doc/tweets2.csv", "r")
# print(df)

# Insert DataFrame to Table

for row in df.itertuples():
    mySql_insert_query = """INSERT INTO ZCtweets_s (id,userid_str,screen_name,created_at,source,in_reply_to_status_id_str,in_reply_to_screen_name,retweet_count,favorite_count,text)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

    recordTuple = (row.id, row.userid_str, row.screen_name, row.created_at,row.source, row.in_reply_to_status_id_str,
                   row.in_reply_to_screen_name, row.retweet_count,row.favorite_count,row.text)
    # recordTuple = ("123", "456", "ghi", "lmn")

    cursor.execute(mySql_insert_query, recordTuple)
    # print(recordTuple)

conn.commit()

