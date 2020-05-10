#####################################################################################################
# Libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, split, col, window, to_timestamp
from pyspark.sql.types import *
import numpy as np
import itertools
import cv2
import sys

#####################################################################################################

# Helper Functions & Variables

# Define Proximity of 1 metre
dist = 1 

# Homography Function
def cv2_homography(src_array, dst_array, x_data, z_data):
    """
    input
    ------------------
    src_pts - the source points in the calibration file, string. 
    dst_pts - the destination points in the calibration file, string.
    x_data - the Head_x location (can be other joint) in the kinect data, float. 
    z_head - the Head_z location (can be other joint) in the kinect data, float.
    output
    ------------------
    dst_point - the new x, z coordinates, list of double
    """
    
    src_pts = np.array(src_array)
    src_pts = src_pts.reshape(9,2)
    dst_pts = np.array(dst_array)
    dst_pts = dst_pts.reshape(9,2)
    
    try:
        M,mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
        # find the location of the second gaze
        pts = np.float32((x_data, z_data)).reshape(-1,1,2)
        point2_transformed = cv2.perspectiveTransform(pts,M)
        dst_point = point2_transformed[0][0]
        return dst_point.tolist()
    
    except:
        print("Unexpected error:", sys.exc_info())

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
        interaction_list =[]
        for pair in pairs:
            first_person_idx =Personlist.index(pair[0])
            second_person_idx =Personlist.index(pair[1])
            print(first_person_idx)
            print(second_person_idx)
            if(map_distance(HeadXlist[first_person_idx],HeadYlist[first_person_idx],HeadXlist[second_person_idx],HeadYlist[second_person_idx])<dist):
                interaction_list.append(list(pair))
        return interaction_list

#####################################################################################################
        
# Create a Spark Session
conf = SparkConf().setAppName('DataStreaming').set("spark.sql.streaming.schemaInference", "true").set("spark.dynamicAllocation.enabled", "false")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Calibration file for homography (to be placed in Hadoop filesystem)
calibration = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("calibration.csv") 
calibration.persist()

# Create a DStream that monitors Hadoop data directory for csv files
df_raw = spark.readStream.option("header", "true").option("inferschema", "true").csv("data")

# Convert timestamp column to timestamp type
df_raw = df_raw.withColumn("new_timestamp", to_timestamp(col("timestamp")))

# Create window operation
df_raw = df_raw.select(window(df_raw.new_timestamp, "30 minutes", "5 minutes"),"*")

#####################################################################################################
    
# Homography  

df_raw = df_raw.join(calibration, "kinect_id")

# change string values to array
df_raw = df_raw.withColumn("src_array", split(col("src"), "\ "))
df_raw = df_raw.withColumn("dst_array", split(col("dst"), "\ "))
    
hmg_udf = udf(cv2_homography, ArrayType(FloatType()))
df_raw = df_raw.withColumn('xz', hmg_udf(df_raw.src_array, df_raw.dst_array, df_raw.Head_x, df_raw.Head_z))
df_raw = df_raw.withColumn('Head_x_homography', df_raw.xz[0]).withColumn('Head_z_homography', df_raw.xz[1])
cols = ['timestamp', 'person_id', 'person_name', 'Head_x_homography', 'Head_z_homography']

df_raw = df_raw.select(*cols)

# Round minute from timestamp
df_raw = df_raw.withColumn('minute', df_raw['timestamp'].substr(0, 16))

#####################################################################################################

# Social interaction analysis

# Define foreach_batch_function for aggregate computations
def foreach_batch_function(df_raw, epoch_id):

    df_raw = df_raw.groupBy("minute","person_name").agg(F.mean('Head_x_homography'),F.mean('Head_z_homography'))
    df_pos = df_raw.sort("minute")
    df_pos1 = df_pos.groupBy('minute').agg(F.collect_list("person_name"))
    df_pos_headXlist = df_pos.groupBy('minute').agg(F.collect_list("avg(Head_x_homography)"))
    df_pos1 = df_pos1.join(df_pos_headXlist, "minute", "outer")
    df_pos_headYlist = df_pos.groupBy('minute').agg(F.collect_list("avg(Head_z_homography)"))
    df_pos1 = df_pos1.join(df_pos_headYlist, "minute", "outer")
    df_raw = df_pos1.orderBy('minute')
    df_raw = df_raw.select(col("minute").alias("Minute"),col("collect_list(person_name)").alias("Personlist"),col("collect_list(avg(Head_x_homography))").alias("HeadXlist"),col("collect_list(avg(Head_z_homography))").alias("HeadYlist"))
    func_udf = udf(func, ArrayType(ArrayType(StringType())))
    df_raw = df_raw.withColumn("interactions", func_udf(df_raw['Personlist'],df_raw['HeadXlist'],df_raw['HeadYlist'])) 

    return df_raw

# Output
    
# Write output to screen for easy debugging
#query = df_raw.writeStream.foreachBatch(foreach_batch_function).outputMode("append").format("console").start()  

# Write out to csv file
query = df_raw.repartition(1).writeStream.foreachBatch(foreach_batch_function).format("csv").option("header", "true").option("checkpointLocation", "checkpoint_final").option("path", "output_final").outputMode("append").trigger(processingTime='300 seconds').start()

#####################################################################################################

# Wait for the computation to terminate                
query.awaitTermination()  

# Stop the computation
query.stop()   