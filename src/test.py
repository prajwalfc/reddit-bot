#!/usr/bin/env python
# coding: utf-8

# In[1]:


sc


# In[52]:


path = "s3n://prajwalfc/dd/temp/temp000000000000"
raw = spark.read.json(path)
raw.show()


# In[53]:


raw.dtypes


# In[54]:


raw1 = raw.select("body","author","subreddit_id","subreddit")
raw1.show()


# In[55]:


rdd = raw1.rdd


# In[56]:


rdd.take(10)


# In[108]:


rdd1 = rdd.map(lambda x:(x['author'],((x['body'],x['subreddit_id'].split('_')[1],x['subreddit']),)))


# In[109]:


rdd1.take(10)


# In[112]:


rdd2 = rdd1.filter(lambda x:x[0]!='[removed]'or x[1][0][0]!='[deleted]').reduceByKey(lambda x,y:x+y).take(10)


# In[105]:


x =(3,)


# In[106]:


len(x)


# In[ ]:




