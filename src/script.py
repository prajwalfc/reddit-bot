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

def compareComments(user_commentsAndSubreddit):
    import lavenshtein,random
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
            if lavenshtein.iterative_levenshtein(comments[i],comments[j])<20:
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
	rdd3.map(compareComments).take(10)
	# rdd3.sqlContext.read.format("jdbc").options(
 #                url="jdbc:postgresql://ec2-100-25-14-184.compute-1.amazonaws.com:5432/test",
 #                dbtable="pr",
 #                driver="org.postgresql.Driver",
 #                user = "postgres",
 #                password="prajwalk",
 #                mode = "append").save()



