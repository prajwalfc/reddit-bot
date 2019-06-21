# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

def iterative_levenshtein(s, t):
    """ 
        iterative_levenshtein(s, t) -> ldist
        ldist is the Levenshtein distance between the strings 
        s and t.
        For all i and j, dist[i,j] will contain the Levenshtein 
        distance between the first i characters of s and the 
        first j characters of t
    """
    rows = len(s)+1
    cols = len(t)+1
    dist = [[0 for x in range(cols)] for x in range(rows)]
    # source prefixes can be transformed into empty strings 
    # by deletions:
    for i in range(1, rows):
        dist[i][0] = i
    # target prefixes can be created from an empty source string
    # by inserting the characters
    for i in range(1, cols):
        dist[0][i] = i
        
    for col in range(1, cols):
        for row in range(1, rows):
            if s[row-1] == t[col-1]:
                cost = 0
            else:
                cost = 1
            dist[row][col] = min(dist[row-1][col] + 1,      # deletion
                                 dist[row][col-1] + 1,      # insertion
                                 dist[row-1][col-1] + cost) # substitution
    
    return dist[row][col]

def compareComments(user_commentsAndSubreddit):
    import random
    sampledCommentsAndSubreddit=[]
    user,commentsAndSubreddit = user_commentsAndSubreddit
    if len(commentsAndSubreddit)==1:
        return user_commentsAndSubreddit+(2,)
    if len(commentsAndSubreddit)>9:
        sampledCommentsAndSubreddit =random.sample(commentsAndSubreddit,k=9)
    else:
        sampledCommentsAndSubreddit=commentsAndSubreddit
    comments=[]
    for commentAndSubreddit in sampledCommentsAndSubreddit:
        comments=comments+[commentAndSubreddit[2]]
    count =0
    for i in range(1,len(comments)):
        for j in range(i+1,len(comments)):
            if iterative_levenshtein(comments[i],comments[j])<20:
                count =count +1
    if count >7:
        for commentAndSubreddit in commentsAndSubreddit:
            return user_commentsAndSubreddit+(1,)
    
def mapper(x):
    user,contents,val=x
    user_content=[]
    for content in contents:
        user_content=user_content+[((user,)+content)+(val,)]
    return user_content

def compareCommentsUserToUser(row):
    val = iterative_levenshtein(row[0][3][:141],row[1][3][:141])
    if val<20:
        return(row[0][:4]+(1,),row[1][:4]+(1,))
    else:
        return row        

if __name__ == "__main__":

	spark = SparkSession.builder.appName("reddit-bot").getOrCreate()
	path = "s3a://prajwalfc/dd/temp/temp000000000000"
	raw = spark.read.json(path).select("body","author","subreddit_id","subreddit").limit(100)
	rdd = raw.rdd
	rdd2 = rdd.map(lambda x: (x[1], [(x[2],x[3],x[0])]))
	rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
	rdd4 = rdd3.map(compareComments)
	rdd5 = rdd4.filter(lambda x:x!=None).map(mapper).flatMap(lambda x:x)#.flatMap(lambda x:x).take(10)
	botRDD = rdd5.filter(lambda x:x[4]==1)
	nonBotRDD = rdd5.filter(lambda x:x[4]==2)
	import re
	appendToBot = nonBotRDD.filter(lambda x:"^I ^am ^a ^bot" in x[3]).map(lambda x:(x[0],x[1],x[2],x[3],1))
	mergedBots=botRDD.union(appendToBot)
	nonBotRDD1 = nonBotRDD.filter(lambda x:"^I ^am ^a ^bot" not in x[3])
	nonBotRDD2 = nonBotRDD1.map(lambda x:(1,x))
	nonBotRDD3 = nonBotRDD2
	joinNonBotRDD = nonBotRDD2.leftOuterJoin(nonBotRDD3).map(lambda x:x[1])\
                          .filter(lambda x:x[0][0]!=x[1][0])\
                          .map(lambda x:((tuple(sorted([x[0][0],x[1][0]]))),x))\
                          .reduceByKey(lambda x,y:y).map(lambda x:x[1])
	userLevelDiffrenetiate = joinNonBotRDD.map(compareCommentsUserToUser).flatMap(lambda x:x)
	df = spark.createDataFrame(userLevelDiffrenetiate.map(lambda x: x[0:3]),schema=["username","subreddit_id","subreddit"])
	df.show()
	df.write.format("jdbc").options(
                url="jdbc:postgresql://ec2-3-219-171-129.compute-1.amazonaws.com:5432/reddit",
                dbtable="botDb",
                driver="org.postgresql.Driver",
                user = "postgres",
                password="prajwalk",
                mode = "append").save()