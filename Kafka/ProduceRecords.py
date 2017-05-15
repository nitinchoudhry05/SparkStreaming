'''
Created on 13-May-2017

@author: nitinchoudhry
'''

from Producer import Producer
import json
import time
import random

class ProduceRecords():
    
    def __init__(self,brokerList,topicName):
        
        self.keys=["key1","key2","key3","key4","key5"]
        self.topicName=topicName
        self.Producer=Producer(brokerList)
        
        
    
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
                
            time.sleep(10)
        
    
    def createValue(self,Key):
        
        Value={"TIMESTAMP":time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(time.time())),
               "val":random.randint(1,10),
               "key":Key
               }
        
        return json.dumps(Value)        
 
 
 
def main():
    
    k=ProduceRecords(["localhost:9092"],"test3")
    k.messgaeCreator()
main()
    
    
               