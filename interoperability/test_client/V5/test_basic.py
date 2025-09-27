import pytest

import mqtt.clients.V5 as mqtt_client, time, logging, socket, sys, getopt, traceback
import mqtt.formats.MQTTV5 as MQTTV5

@pytest.fixture
def base_socket_timeout(pytestconfig):
    return pytestconfig.getoption('base_socket_timeout')

@pytest.fixture
def base_sleep(pytestconfig):
    return pytestconfig.getoption('base_sleep')

@pytest.fixture
def base_wait_for(pytestconfig):
    return pytestconfig.getoption('base_wait_for')

class Callbacks(mqtt_client.Callback):

  def __init__(self):
    self.messages = []
    self.messagedicts = []
    self.publisheds = []
    self.subscribeds = []
    self.unsubscribeds = []
    self.disconnects = []
    self.exceptions = []

  def __str__(self):
     return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
        str(self.subscribeds) + str(self.unsubscribeds) + str(self.disconnects)

  def clear(self):
    self.__init__()

  def disconnected(self, reasoncode, properties):
    logging.info("disconnected %s %s", str(reasoncode), str(properties))
    self.disconnects.append({"reasonCode" : reasoncode, "properties" : properties})

  def connectionLost(self, cause):
    logging.info("connectionLost %s" % str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid, properties=None):
    logging.info("publishArrived %s %s %d %s %d %s", topicName, payload, qos, retained, msgid, str(properties))
    self.messages.append((topicName, payload, qos, retained, msgid, properties))
    self.messagedicts.append({"topicname" : topicName, "payload" : payload,
        "qos" : qos, "retained" : retained, "msgid" : msgid, "properties" : properties})
    return True

  def published(self, msgid, reasonCode=None, properties=None):
    logging.info("published %d", msgid)
    self.publisheds.append((msgid, reasonCode, properties))

  def subscribed(self, msgid, reasonCodes, properties):
    logging.info("subscribed %d", msgid)
    self.subscribeds.append((msgid, reasonCodes, properties))

  def unsubscribed(self, msgid, reasonCodes, properties):
    logging.info("unsubscribed %d", msgid)
    self.unsubscribeds.append((msgid, reasonCodes, properties))

callback = Callbacks()
callback2 = Callbacks()
aclient = mqtt_client.Client("myclientid".encode("utf-8"))
aclient.registerCallback(callback)
bclient = mqtt_client.Client("myclientid2".encode("utf-8"))
bclient.registerCallback(callback2)

topic_prefix = "client_test5/"
topics = [topic_prefix+topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
wildtopics = [topic_prefix+topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
denytopics = [topic_prefix+topic for topic in ["TopicD"]]
nosubscribe_topics = ("test/nosubscribe",)

@pytest.fixture(scope="session", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))
  cleanup(host, port)
  cleanRetained(host, port)

@pytest.fixture(scope="function", autouse=True)
def callbackClear(pytestconfig):
  callback.clear()
  callback2.clear()
  cleanup(pytestconfig.getoption('host'), int(pytestconfig.getoption('port')))

def cleanup(host, port):
    print("clean up starting")
    clientids = ("myclientid", "myclientid2")
    for clientid in clientids:
        curclient = mqtt_client.Client(clientid.encode("utf-8"))
        curclient.connect(host=host, port=port, cleanstart=True)
        time.sleep(.1)
        curclient.disconnect()
        time.sleep(.1)

def cleanRetained(host, port):
  callback = Callbacks()
  curclient = mqtt_client.Client("clean retained".encode("utf-8"))
  curclient.registerCallback(callback)
  curclient.connect(host=host, port=port, cleanstart=True)
  curclient.subscribe(["#"], [MQTTV5.SubscribeOptions(0)])
  time.sleep(2) # wait for all retained messages to arrive
  for message in callback.messages:
    logging.info("deleting retained message for topic %s", message[0])
    curclient.publish(message[0], b"", 1, retained=True)
    waitfor(callback.publisheds, 1, 3)
    assert len(callback.publisheds) == 1
    callback.clear()
  curclient.disconnect()
  time.sleep(.1)

def waitfor(queue, depth, limit):
  total = 0
  while len(queue) < depth and total < limit:
    interval = .5
    total += interval
    time.sleep(interval)

def waitfor_either(condition1_func, condition2_func, limit):
  """
  Wait for either of two conditions to be met within the given time limit.

  Args:
    condition1_func: Function that returns True if condition 1 is met
    condition2_func: Function that returns True if condition 2 is met
    limit: Maximum time to wait in seconds

  Returns:
    tuple: (condition_met, condition_number) where condition_number is 1 or 2
  """
  total = 0
  interval = .5

  while total < limit:
    if condition1_func():
      return (True, 1)
    if condition2_func():
      return (True, 2)

    total += interval
    time.sleep(interval)

  return (False, None)
