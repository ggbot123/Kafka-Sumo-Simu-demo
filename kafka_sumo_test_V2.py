import traci
import traci.constants as tc
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import datetime
import json
import threading
import logging

sumoBinary = "E:\\sumo\\bin\\sumo-gui.exe"
sumoCmd = [sumoBinary, "-c", "E:\\workspace\\SUMO\\AD208demo\\AD208demo.sumocfg"]
logFile = '.\\log\\sys_%s.log' % datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H-%M')
logging.basicConfig(filename=logFile, level=logging.INFO)
logging.info('Simulation start')

# kafka producer thread
class kafkaProducer(threading.Thread):
    def __init__(self, msg):
        self.Ts = 0.2
        self.msg = msg
        threading.Thread.__init__(self)

    def run(self):
        logging.info("[Info] Kafka producer begins running at %s" % time.ctime())
        # print('[Info] Kafka producer begins running at ', time.ctime())
        kp_pub = KafkaProducer(bootstrap_servers=["192.168.104.18:9092"],
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        while True:
            kp_pub.send('Rc_VEH_Data_Basic', self.msg, key=b'vehicleId')
            time.sleep(self.Ts)
    
# kafka consumer thread
class kafkaConsumer(threading.Thread):
    def __init__(self, msg):
        self.Ts = 0.2
        self.msg = msg
        self.isConnected = False
        threading.Thread.__init__(self)

    def run(self):
        group_id = 'gsr_test'
        consumer = KafkaConsumer('Rc_VEH_Data_Basic',
                                enable_auto_commit=False,
                                group_id=group_id,
                                #  auto_offset_reset = 'earliest',
                                bootstrap_servers=["192.168.104.18:9092"])
        logging.info("[Info] Kafka consumer begins running at %s" % time.ctime())
        print('[Info] Kafka consumer begins running at ', time.ctime())
        while True:
            for msg in consumer:
                if self.isConnected == False:
                    logging.info("[Info] Kafka consumer connected at %s" % time.ctime())
                    self.isConnected = True
                    print('[Info] Kafka consumer connected at', time.ctime())
                value = json.loads(msg.value.decode(encoding='utf-8'))
                # demo中在这里模拟云端算法计算
                logging.info("Vehicle State: %s" % value)
                self.msg = {vehId: CtrlInput(value[vehId].velocity[0]) for vehId in value}
            time.sleep(self.Ts)

# virtual vehicle
class Vehicle():
    def __init__(self, vehId):
        self.vehId = vehId
        self.position = traci.vehicle.getPosition(vehId)
        self.velocity = traci.vehicle.getSpeed(vehId)

# control input
class CtrlInput():
    def __init__(self, value):
        self.velocity = value.velocity

# sumo environment
class SumoEnv(threading.Thread):
    def __init__(self):
        self.Ts = 0.2
        self.vehicleDict = {vehId: Vehicle(vehId) for vehId in traci.vehicle.getIDList()}
        self.producer = kafkaProducer(None)
        self.consumer = kafkaConsumer(None)
        threading.Thread.__init__(self)
    
    def update(self, ctrlInputDict):
        for vehId in self.vehicleDict:
            # 假定控制量是车速
            if ctrlInputDict[vehId] is not None:
                traci.vehicle.setSpeed(vehId, ctrlInputDict[vehId][0])
        traci.simulationStep()

    def run(self):
        traci.start(sumoCmd)
        traci.simulationStep()
        self.producer.start()
        self.consumer.start()
        while True:
            self.producer.msg = self.vehicleDict
            logging.info('[%s] msg from kafka producer:\n%s\n' % (str(time.time()), str(self.vehicleDict)))
            time.sleep(self.Ts)
            ctrlInputDict = self.consumer.msg
            logging.info('[%s] msg from kafka consumer:\n%s\n' % (str(time.time()), str(ctrlInputDict)))
            self.update(ctrlInputDict)

# main thread
if __name__ == '__main__':
    env = SumoEnv()
    env.start()
    env.join()
    traci.close()