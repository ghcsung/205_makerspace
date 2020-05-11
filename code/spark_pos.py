#####################################################################################################
# Libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, split, col
from pyspark.sql.types import *
import numpy as np
import itertools
import cv2
import sys
from multiprocessing.pool import ThreadPool
#####################################################################################################

# Helper Functions & Variables

# Define Proximity of 1 metre
dist = 1 

# Function to calculate Euclidean distance
def map_distance(x1,y1,x2,y2):
    dist = np.sqrt((x1-x2)**2+(y1-y2)**2)
    return dist

# Function to create interation list    
def func(Personlist, HeadXlist, HeadYlist):
    if (len(Personlist)==1):
        return []
    else:
        pairs = list(itertools.combinations(Personlist,2))
        pool = ThreadPool(5)
        
        def distance_get(pair):
            first_person_idx =Personlist.index(pair[0])
            second_person_idx =Personlist.index(pair[1])
            if map_distance(HeadXlist[first_person_idx],HeadYlist[first_person_idx],HeadXlist[second_person_idx],HeadYlist[second_person_idx]) < 15:
                return list(pair)        
        return pool.map(distance_get, pairs)
    

#####################################################################################################

# Social interaction analysis

# Create a Spark Session
conf_pos = SparkConf().setAppName('PosStreaming').set("spark.sql.streaming.schemaInference", "true")
sc_pos = SparkContext(conf = conf_pos)
spark_pos = SparkSession(sc_pos)

# Create a DStream that monitors output homography directory for csv files
df_raw2 = spark_pos.readStream.option("header", "true").option("inferschema", "true").csv("output_homography")

# Define foreach_batch_function for aggregate computations
def foreach_batch_function(df_raw2, epoch_id):
    df_raw2 = df_raw2.withColumn('minute', df_raw2['timestamp'].substr(0, 16))
    df_pos_Head_xy = df_raw2.groupBy("minute","person_name").agg(F.mean('Head_x_homography'),F.mean('Head_z_homography'))
    df_pos= df_pos_Head_xy.sort("minute")
    df_pos1 = df_pos.groupBy('minute').agg(F.collect_list("person_name"))
    df_pos_headXlist=df_pos.groupBy('minute').agg(F.collect_list("avg(Head_x_homography)"))
    df_pos1=df_pos1.join(df_pos_headXlist, "minute", "outer")
    df_pos_headYlist=df_pos.groupBy('minute').agg(F.collect_list("avg(Head_z_homography)"))
    df_pos1=df_pos1.join(df_pos_headYlist, "minute", "outer")
    df_pos1=df_pos1.orderBy('minute')
    df_pos1 = df_pos1.select(col("minute").alias("Minute"),col("collect_list(person_name)").alias("Personlist"),col("collect_list(avg(Head_x_homography))").alias("HeadXlist"),col("collect_list(avg(Head_z_homography))").alias("HeadYlist"))
    func_udf = udf(func, ArrayType(ArrayType(StringType())))
    df_raw2 = df_pos1.withColumn("interactions", func_udf(df_pos1['Personlist'],df_pos1['HeadXlist'],df_pos1['HeadYlist'])) 
    
    return df_raw2

# Output
    
# Write output to screen for easy debugging
#pos_query = df_raw2.writeStream.foreachBatch(foreach_batch_function).outputMode("append").format("console").start()  

# Write out to csv file
pos_query = df_raw2.repartition(1).writeStream.foreachBatch(foreach_batch_function).format("csv").option("header", "true").option("checkpointLocation", "checkpoint_final").option("path", "output_final").outputMode("append").trigger(processingTime='300 seconds').start()


# Wait for the computation to terminate          
pos_query.awaitTermination()  
# Stop the computation
pos_query.stop()   