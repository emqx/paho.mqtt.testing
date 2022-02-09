from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, time

# These need to be imported explicitly so that pytest sees it
from .test_basic import base_socket_timeout, base_sleep, base_wait_for


@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

@pytest.mark.rlog_flaky
def test_session_present(base_sleep):
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 5
  # [MQTT-3.2.2-2]
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  time.sleep(1 * base_sleep)
  # [MQTT-3.2.2-3]
  connack = aclient.connect(host=host, port=port, cleanstart=False)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True
  aclient.disconnect()

def test_session_expiry():
  # no session expiry property == never expire
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 0
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.disconnect()

  # session should immediately expire
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

  connect_properties.SessionExpiryInterval = 5
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.disconnect()

  time.sleep(2)
  # session should still exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True
  aclient.disconnect()

  time.sleep(6)
  # session should not exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

  connect_properties.SessionExpiryInterval = 1
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 5
  aclient.disconnect(properties = disconnect_properties)

  time.sleep(3)
  # session should still exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True
  disconnect_properties.SessionExpiryInterval = 0
  aclient.disconnect(properties = disconnect_properties)

  # session should immediately expire
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

def test_assigned_cliend_id():
  # [MQTT-3.2.2-16]
  client = mqtt_client.Client("")
  connack = client.connect(host=host, port=port)
  assert connack.reasonCode.getName() == "Success"
  assert hasattr(connack.properties, "AssignedClientIdentifier") and connack.properties.AssignedClientIdentifier != ''
  client.disconnect()

@pytest.mark.rlog_flaky
def test_maximum_packet_size(base_wait_for, base_sleep, base_socket_timeout):
  callback.clear()
  # [MQTT-3.2.2-15]
  # 1. server max packet size
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                            socket_timeout=9 * base_socket_timeout)
  assert hasattr(connack.properties, "MaximumPacketSize")

  # publish a simple message to ensure that the channel connection
  # state is not idle
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(2 * base_sleep)
  aclient.publish(topics[0], b".", 0)
  time.sleep(2 * base_sleep)
  waitfor(callback.messages, 1, 5 * base_wait_for)
  callback.clear()

  payload = b"." * (int(connack.properties.MaximumPacketSize) + 1)
  aclient.publish(topics[0], payload, 0)
  # should get back a disconnect with packet size too big
  waitfor(callback.disconnects, 1, 9 * base_wait_for)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 149

def test_receive_maximum():
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.ReceiveMaximum=0
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.value == 130
