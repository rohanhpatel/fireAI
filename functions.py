from datetime import datetime
import os

class TimeKeeper:
    def __init__(self, file_name):
        self.name = file_name
        self.log_name = file_name + "_" + datetime.now().strftime("%m-%d-%Y_%H-%M-%S") + ".log"
        self.events = []

    def reset(self):
        self.events.clear()

    def log(self, s):
        with open(os.path.join("log", self.log_name), 'a') as logFile:
            logFile.write(s + "\n")

    def out(self, s):
        nowObj = self.record()
        outString = nowObj.strftime("%m-%d-%Y %H:%M:%S") + f" - {self.name}: " + s
        print(outString)
        self.log(outString)

    def record(self):
        nowObj = datetime.now()
        self.events.append(nowObj)
        return nowObj
    
    def end(self):
        endTime = self.record()
        outString = ""
        if len(self.events) < 2:
            outString = endTime.strftime("%m-%d-%Y %H:%M:%S") + " - TimeKeeper: cannot report last difference since there are less than 2 events"
            
        else:
            outString = endTime.strftime("%m-%d-%Y %H:%M:%S") + f" - TimeKeeper: It took {self.events[-1] - self.events[-2]} to complete the last event"
        print(outString)
        self.log(outString)