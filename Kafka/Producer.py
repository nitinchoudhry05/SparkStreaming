'''
Created on 15-May-2017

@author: nitinchoudhry

'''


from kafka import KeyedProducer,KafkaProducer
from kafka.errors import KafkaError
from kafka.client import SimpleClient

class Producer(object):
    
    '''
    Basic kafka Producer Class to create Producer and send messages to kafka topic
    '''
    def __init__(self, bootstrapservers):
        '''
        Constructor
        '''
        self.Servers=bootstrapservers
        self.Producer=KafkaProducer(bootstrap_servers=self.Servers,retries=5)
        self.client=SimpleClient(bootstrapservers)
            
    
    
    def sendData(self,Key=None,Value=None,TopicName=None):
        
        
        self.client.ensure_topic_exists(TopicName)## This Method ensures whether topic exists ,if not will create a topic in kafka. 
        print "sending message %s for key %s from topic %s"%(Value,Key,TopicName)
        record=self.Producer.send(topic=TopicName, value=Value, key=Key, partition=None, timestamp_ms=None)
        
        try:
            record_metadata = record.get(timeout=10)
            
        except KafkaError,e:
            print str(e)
            # Decide what to do if produce request failed...
            #log.exception()
        
