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

#####################################################################################################

# Helper Functions & Variables

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


#####################################################################################################
        
# Homography  
        
# Create a Spark Session
conf_homo = SparkConf().setAppName('HomoStreaming').set("spark.sql.streaming.schemaInference", "true")
sc_homo = SparkContext(conf = conf_homo)
spark_homo = SparkSession(sc_homo)

# Create a DStream that monitors Hadoop data directory for csv files
df_raw = spark_homo.readStream.option("header", "true").option("inferschema", "true").csv("data")

# Calibration file for homography (to be placed in Hadoop filesystem)
calibration = spark_homo.read.format("csv").option("header", "true").option("inferschema", "true").load("calibration.csv") 

# Join files
calibration.persist()
df_raw = df_raw.join(calibration, "kinect_id")

# change string values to array
df_raw = df_raw.withColumn("src_array", split(col("src"), " ").alias("src"))
df_raw = df_raw.withColumn("dst_array", split(col("dst"), " ").alias("dst"))
    
hmg_udf = udf(cv2_homography, ArrayType(DoubleType()))
df_homo = df_raw.withColumn('xz', hmg_udf(df_raw.src_array, df_raw.dst_array, df_raw.Head_x, df_raw.Head_z))
df_homo = df_homo.withColumn('Head_x_homography', df_homo.xz[0]).withColumn('Head_z_homography', df_homo.xz[1])
cols = ['timestamp', 'person_id', 'person_name', 'Head_x_homography', 'Head_z_homography']
df_homo = df_homo.select(*cols)

# Write output to screen for easy debugging
#homo_query = df_homo.writeStream.format("console").start()  

# Write out to csv file
homo_query = df_homo.repartition(1).writeStream.format("csv").option("header", "true").option("checkpointLocation", "checkpoint_homography").option("path", "output_homography").trigger(processingTime='300 seconds').start()

# Wait for the computation to terminate          
homo_query.awaitTermination()  
# Stop the computation
homo_query.stop()   


