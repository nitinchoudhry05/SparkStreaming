from readConfiguration import ReadConfiguration
import pika
import time
import uuid
import json
import threading

class AMQPConnector(object):

    def __init__(self):
        self.configuration = ReadConfiguration()
        self._staticConf = {}
        self._inRrequest = {}
        self._outRequest = {}
        self._corrCallback = {}
        self._observers = {}


    def createConnection(self,servicename):
        print(threading.current_thread().name)
        print(self.configuration.connectionurl)
        #servicename = self.configuration.servicename

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.publisher_Channel = connection.channel()

        #s  elf.sqReader_Channel.basic_consume(self.readRequests,queue=sqQueue, no_ack=False)
        #self.pqReader_Channel.basic_consume(self.readResponses, queue=self.pqQueue, no_ack=True)

        thread = threading.Thread(target = self.process_personal_queue_data, args = (connection,servicename))
        thread.setDaemon(True)
        thread.start()

        thread1 = threading.Thread(target=self.process_queue_data,
                                  args=(connection,servicename))
        thread1.setDaemon(True)
        thread1.start()
        #self.sqReader_Channel.start_consuming()


        try:
            while True:
                pass
        finally:
            connection.close()


    def process_queue_data(self,connection,servicename):
        print("start nrequest consuming" +  threading.current_thread().name)
        self.sqReader_Channel = connection.channel()
        self.sqReader_Channel.basic_qos(prefetch_count=1)

        directrequestexchange = "%s-request-exchange" % (servicename)
        self.sqReader_Channel.exchange_declare(exchange=directrequestexchange, exchange_type='direct')
        self._staticConf["RequestExchange"] = directrequestexchange

        servicequeuename = "%s-SQ" % (servicename)
        sqQueue = self.sqReader_Channel.queue_declare(queue=servicequeuename, auto_delete=True).method.queue
        self.sqReader_Channel.queue_bind(exchange=directrequestexchange, queue=servicequeuename)
        self._staticConf["ServiceQueue"] = sqQueue

        self.sqReader_Channel.basic_consume(self.readRequests, queue=sqQueue, no_ack=False)
        self.sqReader_Channel.start_consuming()

    def readRequests(self,channel, method_frame, properties_frame, body):
        print("reqd requests" + threading.current_thread().name)
        print(channel)
        expirationtime = properties_frame.expiration
        sendtime = properties_frame.timestamp
        self._inRrequest["CorrelationId"] = properties_frame.correlation_id
        self._inRrequest["ReplyToQ"] = properties_frame.reply_to
        self._inRrequest["expirationtime"] = expirationtime
        self._inRrequest["timestamp"] = sendtime

        currenttime = time.time()
        timegap = (currenttime - float(sendtime)) * 1000

        timeremaining = float(expirationtime) - timegap
        if timeremaining <= 0:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            callback = self._observers["OnRequestReceived"]
            response = self.timeout(callback, body, timeremaining)
            # If the service is not able to adhere the request in timeour interval, there is no point in continuing we should cancel all the underlying service requests also
            self._inRrequest.clear()

            if response == "Timeout":
                # ToDO: Cancel Sent Request to underlying Services
                print("message timeout")

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            print("sending ack back")
            self._outRequest.clear()

    def process_personal_queue_data(self,connection,servicename):
        print("start personal consuming" + threading.current_thread().name)
        self.pqReader_Channel = connection.channel()
        fanoutexchange = "%s-cancel-exchange" % (servicename)
        self.pqReader_Channel.exchange_declare(exchange=fanoutexchange, exchange_type='fanout')
        self._staticConf["CancelExchange"] = fanoutexchange

        self.pqQueue = self.pqReader_Channel.queue_declare(auto_delete=True).method.queue
        self.pqReader_Channel.queue_bind(exchange=fanoutexchange, queue=self.pqQueue, routing_key=servicename)
        self._staticConf["PersonalQueue"] = self.pqQueue

        self.pqReader_Channel.basic_consume(self.readResponses, queue=self.pqQueue, no_ack=True)
        self.pqReader_Channel.start_consuming()

    def readResponses(self,channel, method_frame, properties_frame, body):
        print("read response" + threading.current_thread().name)
        print(channel)
        headers = properties_frame.headers
        if 'Cancel' in headers:
            # TODO handle cancel in different thread
            print("Cancel")
        else:
            # Assumption is that all messages are last message
            # TODO: below piece of code should be asynchronouos
            corr_id = properties_frame.correlation_id
            callbackfunction = self._corrCallback[corr_id]
            callbackfunction(body)

    def requestUnderlyingService(self,callback,servicename,message,headers=()):
        # TODO this has to be asynchronous
        print("requestService" + threading.current_thread().name)
        if "CorrelationId" in self._inRrequest:
            corr_id = str(uuid.uuid4())
            properties = pika.BasicProperties(headers = headers, reply_to= self._staticConf["PersonalQueue"], timestamp=self._inRrequest["timestamp"],
                                              expiration = self._inRrequest["expirationtime"], content_type= "application/json", correlation_id=corr_id)
            servicequeuename = "%s-SQ" % (servicename)
            directrequestexchange = "%s-request-exchange" % (servicename)
            self.publisher_Channel.basic_publish(exchange=directrequestexchange,routing_key=servicequeuename,properties=properties, body=json.dumps(message), mandatory=True)
            # TODO: Where to handle if not able to publish the message, underying service is down

            self._outRequest[servicename] = corr_id
            self._corrCallback[corr_id] = callback
        else:
            return

    def sendResponseBack(self,message,headers=()):
        if "CorrelationId" in self._inRrequest:
            properties = pika.BasicProperties(headers = headers, content_type= "application/json", correlation_id=self._inRrequest["CorrelationId"])
            self.sqReader_Channel.basic_publish(exchange='',routing_key=self._inRrequest["ReplyToQ"],properties=properties, body=json.dumps(message), mandatory=True)
        else:
            return



    def addRequestListener(self,requestcallback):
        self._observers["OnRequestReceived"] = requestcallback

    def _process_cancel_events(self):
        return

    def timeout(self,func, args=(), timeout_duration_ms=1, default="Timeout"):
        import threading
        print(threading.current_thread().name)
        print(timeout_duration_ms)
        class InterruptableThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)
                self.result = default
            def run(self):
                try:
                    self.result = func(args)
                except:
                    self.result = default
        it = InterruptableThread()
        it.start()
        it.join(int(timeout_duration_ms) / 1000)
        if it.isAlive():
            return it.result
        else:
            return it.result

