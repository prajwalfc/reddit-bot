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

def epochToYear(x):
    import time
    year = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x[4]))).split("-")[0]
    return (x[0],[x[1:4]+(int(x[4]),)+x[5:]+(year,)])

def botTrackWithCommentFrequencyfunc(row):
    import numpy as np
    time_diff=[]
    user,value = row
    if len(value)<5:
        return (user,value,0)
    else:
        sd=0
        for i in range (0,len(value)-1):
            time_diff.append(abs(value[i+1][3]-value[i][3])) 
        sorted_time_diff = sorted(time_diff)[:7]
        sd = np.std(sorted_time_diff)
        if sd in range(0,300):
            return (user,value,1)
        else:
            return (user,value,0)
def retainRow(row):
    user,records = row[:2]
    newRow=[]
    for record in records:
        newRow += ((user,)+(record),)
    return newRow

def partitionFunc(rows):
    tupleRow = tuple(rows)
    for i in range(0,len(tupleRow)-2):
        for j in range(i+1, len(tupleRow)-1):
            yield(tupleRow[i],tupleRow[j])
def compareComments(userPairs):
    user_one_comment, user_two_comment = userPairs[0],userPairs[1]
    user_one,comment_one = user_one_comment
    user_two,comment_two = user_two_comment
    if iterative_levenshtein(comment_one[:140],comment_two[:140])>30:
        return ((user_one,0),(user_two,0))
    else:
        return ((user_one,1),(user_two,1))

def normalizerMapper(user_lst):
    user,lst =user_lst[0],user_lst[1]
    total_size = len(lst)
    self_bot_claim_count = 0
    for row in lst:
        if "I am a bot" in row[0]:
            self_bot_claim_count += 1
    return (user,lst,self_bot_claim_count/total_size)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("reddit-bot").getOrCreate()
	path = "s3a://prajwalreddit/reddit/reddit_2019_01_000000000000.json"
	rawDf = spark.read.json(path).select("author","body","subreddit_id","subreddit","created_utc","link_id","parent_id","id","score","controversiality")
	
    # Phase 1: Separate out bot's username to botFromCommentFrequency
    selectedFields = rawRdd.map(epochToYear)
	groupByUser = selectedFields.reduceByKey(lambda x, y: x+y)
	labelWithCommentFrequency = groupByUser.map(botTrackWithCommentFrequencyfunc)
	botFromCommentFrequency = labelWithCommentFrequency.filter(lambda x:x[2]==1).map(lambda x: x[0])
	undecidedUsersPhase_1 = labelWithCommentFrequency.filter(lambda x:x[2]==0)
    
    #Self claimed comments analyzeregainRowFormat = undecidedUsersPhase_1.map(retainRow).flatMap(lambda x:x)
	regainRowFormat = undecidedUsersPhase_1.map(retainRow).flatMap(lambda x:x)
	string = "I am a bot"
	selfClaimmedComments = regainRowFormat.map(lambda x: (x[0],[x[1:]])).reduceByKey(lambda x,y:x+y).map(normalizerMapper)
    
    # self claimed bots¶
    selfClaimedBots = selfClaimmedComments.filter(lambda x: x[2]>0.5).map(lambda x:x[0])

    # undecided bots phase 2¶
    undecidedUsersPhase_2 = selfClaimmedComments.filter(lambda x: x[2]<0.5).map(retainRow).flatMap(lambda x:x)
    ########
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
                password="",
                mode = "append").save()