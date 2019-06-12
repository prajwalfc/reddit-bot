#!/usr/bin/env python
# coding: utf-8



sc
raw = spark.read.json(path)

raw1 = raw.select("body","author","subreddit_id","subreddit")



rdd = raw1.rdd


rdd1 = rdd.map(lambda x:(x['author'],((x['body'],x['subreddit_id'].split('_')[1],x['subreddit']),)))


rdd2 = rdd1.filter(lambda x:x[0]!='[removed]'or x[1][0][0]!='[deleted]').reduceByKey(lambda x,y:x+y).take(10)

