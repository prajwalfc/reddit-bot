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


if __name__ == "__main__":
	spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
	path = "s3n://prajwalfc/dd/temp/temp000000000000"
	raw = spark.read.json(path)
	raw1 = raw.select("body","author","subreddit_id","subreddit")
	rdd = raw1.rdd
	rdd1 = rdd.map(lambda x:(x['author'],((x['body'],x['subreddit_id'].split('_')[1],x['subreddit']),)))
	rdd4=rdd1.map(lambda x:(1,x))
	rdd5=rdd4
	rdd6 = rdd5.leftOuterJoin(rdd4)
	rdd6.saveAsTextFile("../out/joinout.txt")
