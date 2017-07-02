import ConfigParser


class ConfigReader(object):

    def __init__(self,configFile):
        self.config = ConfigParser.ConfigParser()
        self.config.read(configFile)



    def getSections(self):

        return self.config.sections()


    def getProperties(self):

        Sections=self.config.sections()

        for section in Sections:




    def setProperty(self,section_name,property_name,vlaue):

        if self.config