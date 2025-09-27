from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, time, errno, sys

# These need to be imported explicitly so that pytest sees it
from .test_basic import base_socket_timeout, base_sleep, base_wait_for


@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_qos(base_wait_for):
  callback.clear()
  #[MQTT-3.3.1-4]
  aclient.connect(host=host, port=port)

  publish = MQTTV5.Publishes()
  publish.fh.QoS = 3
  mqtt_client.main.sendtosocket(aclient.sock, publish.pack())

  waitfor(callback.disconnects, 1, 3 * base_wait_for)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 130

def test_dup():
  aclient.connect(host=host, port=port)
  publish = MQTTV5.Publishes()
  publish.fh.QoS = 0
  publish.fh.DUP = 1
  mqtt_client.main.sendtosocket(aclient.sock, publish.pack())

  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 130

def test_retained_message():
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[2], b"qos 1 retained", 1, retained=True)
  waitfor(callback.publisheds, 1, 3)
  aclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[2]
  assert callback.messages[0][1] == b"qos 1 retained"
  aclient.unsubscribe([topics[2]])
  waitfor(callback.unsubscribeds, 1, 3)

  # [MQTT-3.3.1-5]
  callback.clear()
  aclient.publish(topics[2], b"qos 1 new retained", 1, retained=True)
  waitfor(callback.publisheds, 1, 3)
  aclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[2]
  assert callback.messages[0][1] == b"qos 1 new retained"
  aclient.unsubscribe([topics[2]])
  waitfor(callback.unsubscribeds, 1, 3)

  # [MQTT-3.3.1-8]
  callback.clear()
  aclient.publish(topics[2], b"qos 1 not retained", 1, retained=False)
  waitfor(callback.publisheds, 1, 3)
  aclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[2]
  assert callback.messages[0][1] == b"qos 1 new retained"
  aclient.unsubscribe([topics[2]])
  waitfor(callback.unsubscribeds, 1, 3)

  # [MQTT-3.3.1-6]
  callback.clear()
  aclient.publish(topics[2], b"", 1, retained=True)
  waitfor(callback.publisheds, 1, 3)
  aclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 0
  aclient.disconnect()

def test_retain_handling():
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[2], b"qos 1 retained", 1, retained=True)
  waitfor(callback.publisheds, 1, 3)
  # [MQTT-3.3.1-9]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2,False,False,0)])
  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 1
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(1,False,False,0)])
  waitfor(callback2.messages, 2, 3)
  assert len(callback2.messages) == 2
  bclient.disconnect()

  # [MQTT-3.3.1-10]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2,False,False,1)])
  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 1
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(1,False,False,1)])
  waitfor(callback2.messages, 2, 3)
  assert len(callback2.messages) == 1
  bclient.disconnect()

  # [MQTT-3.3.1-11]
  callback.clear()
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,2)])
  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 0
  bclient.disconnect()

  aclient.publish(topics[2], b"", 1, retained=True)
  waitfor(callback.publisheds, 1, 3)
  assert len(callback.publisheds) == 1
  aclient.disconnect()

@pytest.mark.rlog_flaky
def test_topic(base_wait_for, base_sleep):
  callback.clear()
  # [MQTT-3.3.2-1]
  # with pytest.raises(Exception):
  #   aclient.connect(host=host, port=port, cleanstart=True)
  #   aclient.publish("主题名".encode('gbk'), b"test topic", 1)

  # [MQTT-3.3.2-2]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(wildtopics[0], b"test topic", 1)
  waitfor(callback.disconnects, 1, 3 * base_wait_for)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 144

  # [MQTT-3.3.2-3]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1 * base_sleep)
  aclient.publish(topics[0], b"test topic", 1)
  waitfor(callback.messages, 1, 3 * base_wait_for)
  aclient.disconnect()
  assert callback.messages[0][0] == topics[0]

def test_payload_format_indicator():
  # [MQTT-3.3.2-4]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.PayloadFormatIndicator = 56
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(0.1)
  aclient.publish(topics[0], b"test_payload_format_indicator", 1, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "PayloadFormatIndicator")
  assert callback.messages[0][5].PayloadFormatIndicator == publish_properties.PayloadFormatIndicator

def test_message_expiry_interval():
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 99999
  bclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 999999999
  bclient.disconnect(properties = disconnect_properties)

  time.sleep(1)

  aclient.connect(host=host, port=port, cleanstart=True)
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.MessageExpiryInterval = 1
  aclient.publish("topic/A", b"qos 1 - expire", 1, retained=True, properties=publish_properties)
  aclient.publish("topic/B", b"qos 2 - expire", 2, retained=True, properties=publish_properties)
  publish_properties.MessageExpiryInterval = 6
  aclient.publish("topic/C", b"qos 1 - don't expire", 1, retained=True, properties=publish_properties)
  aclient.publish("topic/D", b"qos 2 - don't expire", 2, retained=True, properties=publish_properties)

  waitfor(callback.messages, 4, 3)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe(["topic/+"], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.messages, 4, 3)
  bclient.disconnect()
  aclient.disconnect()
  # [MQTT-3.3.2-5]
  assert len(callback2.messages) == 2
  # [MQTT-3.3.2-6]
  assert callback2.messages[0][5].MessageExpiryInterval < 6
  assert callback2.messages[1][5].MessageExpiryInterval < 6

  cleanRetained(host, port)

# set mqtt.max_topic_alias = 10 in emqx.conf
def test_client_topic_alias():
  # no server side topic aliases allowed
  # [MQTT-3.3.2-8]
  aclient.connect(host=host, port=port, cleanstart=True)
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = 0 # topic alias 0 not allowed
  aclient.publish(topics[0], "topic alias 0", 1, properties=publish_properties)
  # should get back a disconnect with Topic alias invalid
  waitfor(callback.disconnects, 1, 2)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 148

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.TopicAliasMaximum = 0 # server topic aliases not allowed
  connect_properties.SessionExpiryInterval = 99999
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                                        properties=connect_properties)
  assert hasattr(connack.properties, "TopicAliasMaximum")
  if connack.properties.TopicAliasMaximum == 0:
    aclient.disconnect
    return

  callback.clear()
  # [MQTT-3.3.2-9]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = connack.properties.TopicAliasMaximum + 1
  aclient.publish(topics[0], b"Greater than Topic Alias Maximum", 1, properties=publish_properties)
  waitfor(callback.disconnects, 1, 2)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 148

  callback.clear()
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                                        properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  # [MQTT-3.3.2-12]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = 1
  aclient.publish(topics[0], b"topic alias 1", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1

  aclient.publish("", b"topic alias 2", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 2, 3)
  assert len(callback.messages) == 2
  aclient.disconnect() # should get rid of the topic aliases but not subscriptions

def test_response_topic():
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  # [MQTT-3.3.2-13]
  # with pytest.raises(Exception):
  #   publish_properties.ResponseTopic = "响应主题".encode('gbk')
  #   aclient.connect(host=host, port=port, cleanstart=True)
  #   aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  # [MQTT-3.3.2-14]
  publish_properties.ResponseTopic = wildtopics[0]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 130

  # [MQTT-3.3.2-15]
  publish_properties.ResponseTopic = topics[0]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "ResponseTopic")
  assert callback.messages[0][5].ResponseTopic == publish_properties.ResponseTopic

def test_correlation_data():
  # [MQTT-3.3.2-16]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.CorrelationData = b"2333"

  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "CorrelationData")
  assert callback.messages[0][5].CorrelationData == publish_properties.CorrelationData

def test_user_properties():
  # [MQTT-3.3.2-17] [MQTT-3.3.2-18]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.UserProperty = ("a", "2")
  publish_properties.UserProperty = ("c", "3")

  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert callback.messages[0][5].UserProperty == [("c", "3"), ("a", "2")] or callback.messages[0][5].UserProperty == [("a", "2"), ("c", "3")]

def test_content_type():
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  # [MQTT-3.3.2-19]
  # with pytest.raises(Exception):
  #   publish_properties.ContentType = "内容类型".encode('gbk')
  #   aclient.connect(host=host, port=port, cleanstart=True)
  #   aclient.publish(topics[0], b"test_content_type", 1, retained=False, properties=publish_properties)

  # [MQTT-3.3.2-20]
  publish_properties.ContentType = '2333'
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_content_type", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "ContentType")
  assert callback.messages[0][5].ContentType == publish_properties.ContentType


def test_overlapping_subscriptions():
  # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
  # the server may send back one message with the highest QoS of any matching subscription, or one message for
  # each subscription with a matching QoS.
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 2333
  aclient.connect(host=host, port=port)
  aclient.subscribe([wildtopics[6], wildtopics[0]], [MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(1)], properties=sub_properties)
  waitfor(callback.subscribeds, 2, 3)
  aclient.publish(topics[3], b"overlapping topic filters", 2)
  waitfor(callback.publisheds, 1, 3)
  aclient.disconnect()
  # [MQTT-3.3.4-2]
  assert len(callback.messages) in [1, 2]
  if len(callback.messages) == 1:
    # This server is publishing one message for all matching overlapping subscriptions, not one for each.
    assert callback.messages[0][2] == 2
  else:
    # This server is publishing one message per each matching overlapping subscription.
    assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or (callback.messages[0][2] == 1 and callback.messages[1][2] == 2)
  # [MQTT-3.3.4-3]
  for m in callback.messages:
    assert hasattr(m[5], "SubscriptionIdentifier")
    assert m[5].SubscriptionIdentifier == sub_properties.SubscriptionIdentifier


def test_subscribe_identifiers(base_wait_for):
  aclient.connect(host=host, port=port, cleanstart=True)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 23333
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)
  waitfor(callback.subscribeds, 1, 3 * base_wait_for)

  bclient.connect(host=host, port=port, cleanstart=True)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 2
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)

  sub_properties.clear()
  sub_properties.SubscriptionIdentifier = 3
  bclient.subscribe([topics[0]+"/#"], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)

  waitfor(callback2.subscribeds, 1, 3 * base_wait_for)
  bclient.publish(topics[0], b"sub identifier test", 1, retained=False)

  # [MQTT-3.3.4-4]
  waitfor(callback.messages, 1, 3 * base_wait_for)
  assert len(callback.messages) == 1
  assert callback.messages[0][5].SubscriptionIdentifier[0] == 23333
  aclient.disconnect()

  # [MQTT-3.3.4-5]
  waitfor(callback2.messages, 2, 6 * base_wait_for)
  assert len(callback2.messages) == 2
  expected_subsids = set([2, 3])
  received_subsids = set([callback2.messages[0][5].SubscriptionIdentifier[0],
                          callback2.messages[1][5].SubscriptionIdentifier[0]])
  assert received_subsids == expected_subsids
  bclient.disconnect()

@pytest.mark.rlog_flaky
def test_maximum_packet_size(base_wait_for, base_sleep, base_socket_timeout):
  callback.clear()
  # [MQTT-3.2.2-15]
  # 1. server max packet size
  myclient = mqtt_client.Client("test_maximum_packet_size")
  myclient.registerCallback(callback)
  connack = myclient.connect(host=host, port=port, cleanstart=True,
                             socket_timeout=9 * base_socket_timeout)
  assert hasattr(connack.properties, "MaximumPacketSize" )
  payload = b"." * (int(connack.properties.MaximumPacketSize) + 1)
  sys.stdout.write(f"DEBUG: Publishing oversized payload of {len(payload)} bytes (max allowed: {connack.properties.MaximumPacketSize})\n")
  sys.stdout.flush()
  time.sleep(4 * base_sleep)
  myclient.publish(topics[0], payload, 0)
  # Define condition functions for waitfor_either
  def condition1_disconnect():
    disconnect_count = len(callback.disconnects)
    if disconnect_count >= 1:
      # Use sys.stdout.write to bypass pytest output capture
      sys.stdout.write(f"DEBUG: DISCONNECT condition met, count={disconnect_count}\n")
      sys.stdout.flush()
    return disconnect_count >= 1
  def condition2_exception():
    exception_count = len(callback.exceptions)
    if exception_count >= 1:
      # Use sys.stdout.write to bypass pytest output capture
      sys.stdout.write(f"DEBUG: EXCEPTION condition met, count={exception_count}\n")
      sys.stdout.flush()
    return exception_count >= 1
  # Wait for either proper DISCONNECT or TCP connection reset
  # Use longer timeout for CI environments (GitHub Actions can be slower)
  timeout_seconds = 60 * base_wait_for
  sys.stdout.write(f"DEBUG: Waiting up to {timeout_seconds} seconds for either DISCONNECT or exception\n")
  sys.stdout.flush()
  condition_met, condition_num = waitfor_either(condition1_disconnect, condition2_exception, timeout_seconds)
  if condition_num == 1:
    # case 1: broker sends proper DISCONNECT
    assert len(callback.disconnects) == 1
    assert callback.disconnects[0]["reasonCode"].value == 149
  elif condition_num == 2:
    # case 2: broker just closed TCP
    # allow it as valid too
    assert not callback.disconnects
    assert callback.exceptions  # make sure something got logged
    exc_type, exc_val = callback.exceptions[-1]
    assert exc_type is ConnectionResetError
    assert exc_val.errno == errno.ECONNRESET
  else:
    # Neither condition met within timeout
    sys.stdout.write(f"DEBUG: Timeout reached. Disconnects: {len(callback.disconnects)}, Exceptions: {len(callback.exceptions)}\n")
    if callback.exceptions:
      sys.stdout.write(f"DEBUG: Last exception: {callback.exceptions[-1]}\n")
    sys.stdout.flush()
    assert False, "Neither DISCONNECT nor exception occurred within timeout"
