'''
Created on 15-May-2017

@author: nitinchoudhry
'''
from __future__ import print_function
import sys
sys.path.append("../")
#from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from Kafka.Producer import Producer
import ast
import numpy


class RecordAggregator(object):
    
    
    def __init__(self,appName="KafkaRecordAggregator",inputTopicName="test2",outputTopicName="test_aggregated",bootstramservers="localhost:9092",aggregationInterval=120):
        self.sparkContext = SparkContext("local[2]", appName)
        self.StreamingContext = StreamingContext(self.sparkContext,aggregationInterval)
        self.KafkaStream = KafkaUtils.createDirectStream(self.StreamingContext, [inputTopicName], {"metadata.broker.list": bootstramservers})
        self.Producer=Producer(bootstramservers)
    	self.outputTopicName=outputTopicName
    
    def aggregateRecods(self):
        
        dstream = self.KafkaStream.map(lambda x: (x[0],[json.loads(x[1])["val"],json.loads(x[1])["TIMESTAMP"],1,str(json.loads(x[1])["val"])])).reduceByKey(lambda x,y :[x[0]+y[0],x[1]+","+y[1],x[2]+y[2],x[3]+","+y[3],float(x[0]+y[0])/(x[2]+y[2])])
        
        dstream.pprint(20)


        
        def Send(time,message):
            
            r=message.collect()
            for record in r:
                Key=str(record[0])
                val=ast.literal_eval(str(record[1]))       
                vals=[ int(i) for i in val[3].split(",")]      
                Out={"count":val[2],
                 "TIMESTAMP":str(time),
                  "key":Key,
                  "ts":val[1].split(","),
                  "vals":vals,
                  "mean":numpy.mean(vals),
                  }
		self.Producer.sendData(Key, json.dumps(Out),self.outputTopicName)
            
              
        dstream.foreachRDD(Send)  
        self.StreamingContext.start()
        self.StreamingContext.awaitTermination() 
        
   
    
    
def main():

    if len(sys.argv)<2:
        print ("Insufficient arguments provideed \n Code Usage- spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 RecordAggregator.py <config File>")
        exit()

    else:
        ConfigFile=open(sys.argv[1],"r")
        Configs=ConfigFile.readlines()
        inputTopicname="Default"
	brokerList=["localhost:9092"]
	aggregationWindow=120
	outputTopicName="test_aggregated"
	for config in Configs:
                if "inputTopicName" in config:
                        inputTopicname=config.split("=")[1].strip("\n")

                if "brokerList" in config:
                        brokerList=config.strip("\n").split("=")[1]

                if "aggregationWindow" in config:
                        aggregationWindow=int(config.split("=")[1].strip("\n"))
		
		if "outputTopicName" in config:
                        outputTopicName=(config.split("=")[1].strip("\n"))
	

	#print "Config Sucessfully Captured\n Details:\n1.TopicName=%s\n2.BrokerList=%s\n3.aggregationWindow=%s\n4.outputTopicName=%s"%[inputTopicname,str(brokerList),aggregationWindow,outputTopicName]
    Aggregator=RecordAggregator(inputTopicName=inputTopicname,outputTopicName=outputTopicName,bootstramservers=brokerList,aggregationInterval=aggregationWindow)

    Aggregator.aggregateRecods() 
main()    
