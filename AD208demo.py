import traci
import traci.constants as tc
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import datetime
import json
import threading
import logging

sumoBinary = "C:\\sumo\\bin\\sumo-gui.exe"
sumoCmd = [sumoBinary, "-c", "C:\\workspace\\SUMO\\simu_demo\\AD208demo.sumocfg"]
logFile = '.\\log\\sys_%s.log' % datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H-%M')
logging.basicConfig(filename=logFile, level=logging.INFO)
logging.info('Simulation start')

# kafka producer thread
class kafkaProducer(threading.Thread):
    def __init__(self, vehId):
        self.Ts = 0.2
        self.vehId = vehId
        self.isRunning = True
        threading.Thread.__init__(self)

    def run(self):
        logging.info("[Info] Kafka producer %s begins running at %s\n" % (self.vehId, time.ctime()))
        producer = KafkaProducer(bootstrap_servers=["192.168.104.18:9092"],
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        while self.isRunning:
            json_msg = self.getState()
            logging.info('[%d] msg from kafka producer %s:\n%s\n' % (int(round(time.time()*1000)), str(self.vehId), str(json_msg)))
            producer.send('Rc_VEH_Data_Basic', json_msg, key=b'vehicleId')
            time.sleep(self.Ts)
        producer.close()

    def getState(self):
        X_position = traci.vehicle.getPosition(self.vehId)[0] - traci.vehicle.getLength(self.vehId)/2
        Y_position = traci.vehicle.getPosition(self.vehId)[1]
        X_velocity = traci.vehicle.getSpeed(self.vehId)
        X_accel = traci.vehicle.getAcceleration(self.vehId)
        TimeStamp = int(round(time.time()*1000))
        if X_accel > 10:
            X_accel = 0
        data = {'vehicleId':self.vehId, 'TimeStamp':TimeStamp, 'X_position':X_position, 'Y_position':Y_position, 'X_velocity':X_velocity, 'X_accel':X_accel}
        return data

    def close(self):
        self.isRunning = False
    
# kafka consumer thread
class kafkaConsumer(threading.Thread):
    def __init__(self, msg):
        self.Ts = 0.2
        self.msg = msg
        self.isConnected = False
        self.isRunning = True
        threading.Thread.__init__(self)

    def run(self):
        group_id = 'AD208demo'
        consumer = KafkaConsumer('Edge_Fus_Data_4g',
                                enable_auto_commit=False,
                                group_id=group_id,
                                bootstrap_servers=["192.168.104.18:9092"])
        logging.info("[Info] Kafka consumer begins running at %s\n" % (time.ctime()))
        print('[Info] Kafka consumer begins running at %s\n' % (time.ctime()))
        while self.isRunning:
            for msg in consumer:
                if self.isConnected == False:
                    logging.info("[Info] Kafka consumer connected at %s\n" % (time.ctime()))
                    self.isConnected = True
                    # print('[Info] Kafka consumer %s connected at %s\n' % (self.vehId, time.ctime()))
                self.msg.update(json.loads(msg.value.decode(encoding='utf-8')).get('message', 0)[0].get('data', 0)[0])
                logging.info('[%d] msg to kafka consumer:\n%s\n' % (int(round(time.time()*1000)), str(self.msg)))
            time.sleep(self.Ts)
        consumer.close()
    
    def close(self):
        self.isRunning = False

# sumo environment
class SumoEnv(threading.Thread):
    def __init__(self):
        traci.start(sumoCmd)
        traci.simulationStep()
        time.sleep(3)
        traci.vehicle.setSpeed("C1", 15)
        traci.vehicle.setSpeed("C2", 12)
        traci.vehicle.setSpeed("H0", 12)
        traci.vehicle.setSpeed("H1", 15) 
        self.Ts = 0.2
        self.isRunning = True
        self.vehDict = {}
        self.consumer = kafkaConsumer({})
        threading.Thread.__init__(self)
             
    def updateState(self, ctrlInput):
        for vehId in ctrlInput:
            # 控制量：车速、换道决策
            traci.vehicle.setSpeed(vehId, ctrlInput[vehId]['speed'])
            if ctrlInput[vehId]['flag'] == True:
                laneInd = traci.vehicle.getLaneIndex(vehId)
                traci.vehicle.changeLane(vehId, laneInd + 1, 3)   # 向左换道
                self.isRunning = False
        #traci.simulationStep()
   
    def run(self):
        self.consumer = kafkaConsumer({})
        self.consumer.start()
        while self.isRunning:
            vehIdList = traci.vehicle.getIDList()
            # print("vehIdList: %s" % str(vehIdList))
            # print("self.vehIdList: %s" % str(tuple(self.vehDict.keys())))
            for vehId in (vehIdList + tuple(self.vehDict.keys())):
                if vehId not in self.vehDict:
                    self.vehDict[vehId] = kafkaProducer(vehId)
                    self.vehDict[vehId].start()
                    logging.info("[%d] Vehicle %s enters SumoEnv.\n" % (int(round(time.time()*1000)), vehId))
                    traci.vehicle.setSpeedMode(vehId, 32)
                    traci.vehicle.setLaneChangeMode(vehId, 0b000000000000)
                if vehId not in vehIdList:
                    self.vehDict[vehId].close()
                    del self.vehDict[vehId]
                    logging.info("[%d] Vehicle %s exits SumoEnv.\n" % (int(round(time.time()*1000)), vehId))
                    if len(self.vehDict) == 0:
                        self.isRunning = False
            if self.consumer.isConnected:
                logging.info('[%d] msg from kafka consumer:\n%s\n' % (int(round(time.time()*1000)), str(self.consumer.msg)))
                self.updateState(self.consumer.msg)
            traci.simulationStep()
            time.sleep(self.Ts)         
        logging.info("[%d] Simulation Terminates.\n" % int(round(time.time()*1000)))
        for vehId in self.vehDict:
            self.vehDict[vehId].close()
        self.consumer.close()
        traci.close()

# main thread
if __name__ == '__main__':
    env = SumoEnv()
    env.start()