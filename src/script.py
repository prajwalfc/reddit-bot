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
        ###############To do filter out short comments
        sampledCommentsAndSubreddit =random.sample(commentsAndSubreddit,k=9)
    else:
        sampledCommentsAndSubreddit=commentsAndSubreddit
    comments=[]
    for commentAndSubreddit in sampledCommentsAndSubreddit:
        comments=comments+[commentAndSubreddit[2]]
    count =0
    for i in range(1,len(comments)):
        for j in range(i+1,len(comments)):
            if iterative_levenshtein(comments[i],comments[j])<30:
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
    first_comment = row[0][3][:141]
    second_comment = row[1][3][:141]
    doCompare = len(first_comment.split(" "))>=7 and len(second_comment.split(" "))>=7
    val = -1
    if doCompare:
        val = iterative_levenshtein(row[0][3][:141],row[1][3][:141])
    if val>=0 and val<30:
        return(row[0][:4]+(1,),row[1][:4]+(1,))
    else:
        return (row[0][:4]+(1,),row[1][:4]+(0,))


if __name__ == "__main__":
    spark = SparkSession.builder.appName("reddit-bot").getOrCreate()
    path = "s3a://testbucketforprajwal/temp/temp000000000000"
    rawDf = spark.read.json(path).select("body", "author", "subreddit_id", "subreddit").limit(100)
    rawRdd = rawDf.rdd
    selectedFields = rawRdd.map(lambda x: (x[1], [(x[2], x[3], x[0])]))
    groupByUser = selectedFields.reduceByKey(lambda x, y: x + y)
    # group by users and see if they have been posting similar type of comments
    usersSelfComments = groupByUser.map(compareComments)
    # filter out nones which means we don't actually know if they are bots or not
    botsOrNonBots = usersSelfComments.filter(lambda x: x != None).map(mapper).flatMap(lambda x: x)
    botRDD = botsOrNonBots.filter(lambda x: x[4] == 1)
    nonBotRDD = botsOrNonBots.filter(lambda x: x[4] == 2)
    appendToBot = nonBotRDD.filter(lambda x: "^I ^am ^a ^bot" in x[3]).map(lambda x: (x[0], x[1], x[2], x[3], 1))
    mergedBots = botRDD.union(appendToBot)
    nonBotRDDNotSure = nonBotRDD.filter(lambda x: "^I ^am ^a ^bot" not in x[3])
    nonBotRDDNotSureLeftTable = nonBotRDDNotSure.map(lambda x: (1, x))
    nonBotRDDNotSureRightTable = nonBotRDDNotSureLeftTable
    joinBotNotSureRDD = nonBotRDDNotSureLeftTable.leftOuterJoin(nonBotRDDNotSureRightTable).map(lambda x: x[1]) \
        .filter(lambda x: x[0][0] != x[1][0]) \
        .map(lambda x: ((tuple(sorted([x[0][0], x[1][0]]))), x)) \
        .reduceByKey(lambda x, y: y).map(lambda x: x[1])
    userLevelDiffrenetiate = joinBotNotSureRDD.map(compareCommentsUserToUser).flatMap(lambda x: x)
    df = spark.createDataFrame(userLevelDiffrenetiate.map(lambda x: x[0:3]),schema=["username","subreddit_id","subreddit"])
    df.show()
    df.write.format("jdbc").options(
                url="jdbc:postgresql://ec2-3-219-171-129.compute-1.amazonaws.com:5432/reddit",
                dbtable="botDb1",
                driver="org.postgresql.Driver",
                user = "postgres",
                password="prajwalk",
                mode = "append").save()