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

def get connectionObject()

if __name__ == "__main__":

	spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
	path = "s3n://prajwalfc/dd/temp/temp000000000000"
	raw = spark.read.json(path).select("body","author","subreddit_id","subreddit").limit(100)
	rdd = raw.rdd
	rdd2 = rdd.map(lambda x: (x[1], [(x[2],x[3],x[1])]))
	rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
	rdd3.sqlContext.read.format("jdbc").options(
                url="jdbc:postgresql://ec2-100-25-14-184.compute-1.amazonaws.com:5432/test",
                dbtable="pr",
                driver="org.postgresql.Driver",
                user = "postgres",
                password="prajwalk",
                mode = "append").save()

