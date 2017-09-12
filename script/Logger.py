import logging
import logging.config
class MyLogger(object):
    
    def __init__(self, loggerName="customLogger", loggerLevel=logging.DEBUG):
        #self.logger = logging.getLogger(loggerName)
        #self.logger.setLevel(logging.DEBUG)
        
        #consoleHandler = logging.StreamHandler()
        #consoleHandler.setLevel(loggerLevel) 
        
        #formatter = logging.Formatter("%(asctime)s-[ %(levelname)s ]: %(message)s")
        
        #consoleHandler.setFormatter(formatter)
        
        #self.logger.addHandler(consoleHandler)

        
        logging.config.fileConfig("logging.conf") 
        self.logger = logging.getLogger(loggerName) 
        
    def getLogger(self):
        return self.logger
        
        


def runLoggerTest():
    logger = MyLogger("Simple_Logger").getLogger()
    
    logger.debug("This is debug message")
    logger.info("This is info message")
    logger.warning("This is a warn message")
    logger.error("This is a error message")
    logger.critical("This is a critical message")
