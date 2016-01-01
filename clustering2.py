import os
import sys
# Path for spark source folder
# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/1.5.2/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/1.5.2/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/1.5.2/python/lib/py4j-0.8.2.1-src.zip')
import multiprocessing
import time
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
import matplotlib.pyplot as plt
import matplotlib.patches as patches
clusterNum=15

def data_plotting(q):
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(111)
    plt.ion() # Interactive mode

    fig.show()
    #fig.tight_layout()
    tx=plt.text(0,30, '')
    colors = plt.get_cmap('jet')(np.linspace(0.0, 1.0, clusterNum))

    while True:
        if q.empty():
            time.sleep(5)

        else:
            plt.cla()
            plt.ylim([0,60])
            plt.xlim([0,60])
            x=np.asarray([[20,20],[20 ,40],[40,40],[40,20]])
            plt.scatter(x[:,0], x[:,1],  marker='x', alpha = 0.5,color='b')
            # p = patches.Rectangle((-10,-10), 50, 50, linewidth=0, fill=True, color='w')
            # p.set_alpha(0.1)
            #ax.add_patch(p)
            obj=q.get() # (DenseVector([6.7456, 7.7456]), 0)
            d=[x[0] for x in obj]
            c=[x[1] for x in obj]
            data = np.array(d)
            pcolor=np.array(c)

            try:

                plt.scatter(data[:,0], data[:,1],  marker='o', alpha = 0.5,color=colors[pcolor])
                # for x in obj:
                #     plt.text(x[0][0], x[0][1],x[1])
                tx.set_text(str(data[0]))
                plt.pause(0.0001)
                plt.draw()
                time.sleep(5)
            except IndexError: # Empty array
                pass

            # if not f.empty():
            #     objtopic=f.get() #(11, ([-49.261209, -16.6427825], [('phlima07', 1), ('dm', 1), ('ouuu', 1)]))
            #     for row in objtopic:
            #         #print(row)
            #         #xc,yc=ax1(row[1][0][0], row[1][0][1])
            #         #print (xc)
            #         clus=row[0]
            #         #ptext[clus].set_text(str(clus)+ ':'+str([x[0] for x in row[1][1]]))
            #         ptext[clus].set_text(str(clus)+ ':'+str(row[1][1]))
            #         ptext[clus].set_color(colors[clus])
            #     plt.pause(0.0001)
            #


q = multiprocessing.Queue()
f = multiprocessing.Queue()
job_for_another_core2 = multiprocessing.Process(target=data_plotting,args=(q,))
job_for_another_core2.start()

sc  = SparkContext('local[4]', 'Social Panic Analysis')
# Create a local StreamingContext with two working thread and batch interval of 1 second

ssc = StreamingContext(sc, 10)
dstream = ssc.socketTextStream("localhost", 9998)
trainingData = dstream.map(Vectors.parse)
trainingData.pprint()
testData=trainingData.map(lambda x: (x,x))
testData.pprint()
model = StreamingKMeans(k=clusterNum, decayFactor=0.1).setRandomCenters(2, 1.0, 0)
model.trainOn(trainingData)
print(model.latestModel().clusterCenters)
clust=model.predictOnValues(testData)
clust.pprint()
#print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))
clust.foreachRDD(lambda time, rdd: q.put(rdd.collect()))
ssc.start()
ssc.awaitTermination()