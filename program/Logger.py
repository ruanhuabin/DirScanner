import logging
import logging.config
class MyLogger(object):
    
    def __init__(self, loggerName="customLogger", loggerLevel=logging.DEBUG, logFileName="mrc.log"):
        #self.logger = logging.getLogger(loggerName)
        #self.logger.setLevel(logging.DEBUG)
        
        #consoleHandler = logging.StreamHandler()
        #consoleHandler.setLevel(loggerLevel) 
        
        #formatter = logging.Formatter("%(asctime)s-[ %(levelname)s ]: %(message)s")
        
        #consoleHandler.setFormatter(formatter)
        
        #self.logger.addHandler(consoleHandler)

        
        #logging.config.fileConfig("logging.conf") 
        #self.logger = logging.getLogger(loggerName) 

        self.logger = logging.getLogger(loggerName)
        self.logger.setLevel(loggerLevel)
        
        fileHandler = logging.FileHandler(logFileName)
        fileHandler.setLevel(loggerLevel)

        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(filename)s:%(funcName)s:%(lineno)d]-%(asctime)s-%(name)s-%(levelname)s: %(message)s')

        fileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)

        self.logger.addHandler(consoleHandler)
        self.logger.addHandler(fileHandler)

        
    def getLogger(self):
        return self.logger
        
        


def runLoggerTest():
    logger = MyLogger("Simple_Logger").getLogger()
    
    logger.debug("This is debug message")
    logger.info("This is info message")
    logger.warning("This is a warn message")
    logger.error("This is a error message")
    logger.critical("This is a critical message")
