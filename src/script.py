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
            return(user,commentAndSubreddit[0],commentAndSubreddit[1],1)
    

if __name__ == "__main__":

	spark = SparkSession.builder.appName("reddit-bot").getOrCreate()
	path = "s3n://prajwalfc/dd/temp/temp000000000000"
	raw = spark.read.json(path).select("body","author","subreddit_id","subreddit").limit(100)
	rdd = raw.rdd
	rdd2 = rdd.map(lambda x: (x[1], [(x[2],x[3],x[0])]))
	rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
	rdd3.map(compareComments).toDF().sqlContext.read.format("jdbc").options(
                url="jdbc:postgresql://ec2-3-219-171-129.compute-1.amazonaws.com:5432/reddit",
                dbtable="test",
                driver="org.postgresql.Driver",
                user = "postgres",
                password="prajwalk",
                mode = "append").save()



