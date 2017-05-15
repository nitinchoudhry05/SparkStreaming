'''
Created on 15-May-2017

@author: nitinchoudhry
'''

from Producer import Producer
import json
import time
import random
import sys
class ProduceRecords():
    
    def __init__(self,brokerList,topicName,interval=30):
        
        self.keys=["key1","key2","key3","key4","key5"]
        self.topicName=topicName
        self.Producer=Producer(brokerList)
	self.interval=interval
        
        
    
    def messgaeCreator(self):
        
        
        Status=True
        while Status:
            for key in self.keys:
                try:
                    self.Producer.sendData(key, self.createValue(key), self.topicName)
                    time.sleep(random.randint(1,4))
                except Exception,e:
                    print "Exception Encountered"
                    print e
                    exit()
                
            time.sleep(int(self.interval))
        
    
    def createValue(self,Key):
        
        Value={"TIMESTAMP":time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(time.time())),
               "val":random.randint(1,10),
               "key":Key
               }
        
        return json.dumps(Value)        
 
 
 
def main():

    if len(sys.argv)<2:
	print "Insufficient arguments provideed \n Code Usage- python ProduceRecords.py <config File>"
	exit()    
    else:
	ConfigFile=open(sys.argv[1],"r")
	Configs=ConfigFile.readlines()
	inputTopicName="Default"
	brokerList=["localhost:9092"]
	timeInterVal=30	
	for config in Configs:
		if "inputTopicName" in config:
			inputTopicName=config.split("=")[1].strip("\n")
		
		if "brokerList" in config:
                        brokerList=config.split("=")[1].strip("\n").split(",")
		
		if "timeInterVal" in config:
                        timeInterVal=int(config.split("=")[1].strip("\n"))

		
    
    print "Config Sucessfully Captured\n Details:\n1.TopicName=%s\n2.BrokerList=%s\n3.TimeInterval=%s"%(inputTopicName,str(brokerList),timeInterVal)
    
    Producer=ProduceRecords(brokerList,inputTopicName,timeInterVal)
    Producer.messgaeCreator()
main()
    

