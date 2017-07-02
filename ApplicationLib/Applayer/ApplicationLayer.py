import sys
sys.path.append("../")

import utils.configReader
import pika

class ApplicationLayer(object)

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.Publish=self.connection.channel(1)
        