import os
import sys
# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(',')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
    return LabeledPoint(label, vec)


sc  = SparkContext('local[4]', 'Social Panic Analysis')
# Create a local StreamingContext with two working thread and batch interval of 1 second

ssc = StreamingContext(sc, 10)
trainingData = ssc.textFileStream("./training/").map(Vectors.parse)
trainingData.pprint()
testData = ssc.textFileStream("./testing/").map(parse)
testData.pprint()
model = StreamingKMeans(k=5, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
model.trainOn(trainingData)
model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))).pprint()
#print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))

ssc.start()
ssc.awaitTermination()