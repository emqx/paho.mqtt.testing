import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3

from test_client.V311.test_basic import *

import pytest
import functools
import time
import socket
import sys

@pytest.fixture(scope="class", autouse=True)
def __cleanup(pytestconfig):
    global host, port
    host = pytestconfig.getoption('host')
    port = int(pytestconfig.getoption('port'))
    cleanup(["myclientid", "myclientid2"], host=host, port=port)

class TestPublish():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self):
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]

    # Set zone.external.retry_interval = 2s in emqx.conf
    def test_dup(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        callback2 = Callbacks()
        client2 = mqtt_client.Client("myclientid2")

        client.connect(host=host, port=port, cleansession=True)
        client2.connect(host=host, port=port, cleansession=False)

        client2.subscribe([self.topics[0]], [1])
        time.sleep(0.5)
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client2.sock))
        assert packet.fh.MessageType == MQTTV3.SUBACK

        client.publish(self.topics[0], b"dup is 0", 1)
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client2.sock))
        assert packet.fh.MessageType == MQTTV3.PUBLISH
        assert packet.fh.DUP == False
        puback = MQTTV3.Pubacks()
        puback.messageIdentifier = packet.messageIdentifier
        client2.sock.send(puback.pack())

        client.publish(self.topics[0], b"dup is 1", 1)
        # Receive but do not reply
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client2.sock))
        packet = None
        spent = 0
        while spent < 6:
            try:
                packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client2.sock))
            except:
                assert sys.exc_info()[0] == socket.timeout
            spent += 0.5
            if packet != None:
                break
        assert packet.fh.MessageType == MQTTV3.PUBLISH
        assert packet.fh.DUP == True
        
        client.disconnect()
        client2.disconnect()

    def test_invalid_qos(self):
        client = mqtt_client.Client("myclientid")
        client.connect(host=host, port=port, cleansession=True)
        client.publish(self.topics[0], b"bad qos", qos=3)
        assert b'' == client.sock.recv(1)

    def test_retain_message(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=host, port=port, cleansession=True)
        client.subscribe([self.topics[0]], [0])
        time.sleep(0.5)
        client.publish(self.topics[0], b"retain message", qos = 0, retained = True)
        waitfor(callback.messages, 1, 1)
        # [MQTT-3.3.1-9]
        assert callback.messages[0][3] == False

        client.unsubscribe([self.topics[0]])
        callback.clear()
        client.subscribe([self.topics[0]], [0])
        waitfor(callback.messages, 1, 1)
        # [MQTT-3.3.1-6], [MQTT-3.3.1-8]
        assert callback.messages[0][3] == True

        client.unsubscribe([self.topics[0]])
        callback.clear()
        # [MQTT-3.3.1-10]
        client.publish(self.topics[0], b"", qos = 1, retained = True)
        waitfor(callback.publisheds, 1, 1)
        client.subscribe([self.topics[0]], [0])
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 1, 1)
        client.disconnect()
        callback.clear()

    def test_topic_name(self):
        # [MQTT-3.3.2-2]
        client = mqtt_client.Client("myclientid")
        client.connect(host=host, port=port, cleansession=True)
        client.publish(self.wildtopics[0], b"topic contains wildcard characters", qos = 0)
        assert b'' == client.sock.recv(1)

    def test_actions(self):
        clientid1 = "client-test_actions-1"
        clientid2 = "client-test_actions-2"
        clientid3 = "client-test_actions-3"
        client = mqtt_client.Client(clientid1)
        client.connect(host=host, port=port, cleansession=True)

        # QoS 0
        client.publish(self.topics[0], b"qos 0", qos = 0)
        succeeded = False
        try:
            packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        except:
            assert sys.exc_info()[0] == socket.timeout
            succeeded = True
        assert succeeded == True

        # QoS 1
        client.publish(self.topics[0], b"qos 1", qos = 1)
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.PUBACK

        # Client -> Server: PUBLISH
        client.publish(self.topics[0], b"qos 2", qos = 2)
        # Server -> Client: PUBREC
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.PUBREC
        # Client -> Server: PUBREL
        pubrel = MQTTV3.Pubrels()
        pubrel.messageIdentifier = packet.messageIdentifier
        client.sock.send(pubrel.pack())
        # Server -> Client: PUBCOMP
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.PUBCOMP

        callback2 = Callbacks()
        client2 = mqtt_client.Client(clientid2, callback2)
        client2.connect(host=host, port=port, cleansession=True)
        client.subscribe([self.topics[0]], [2])
        MQTTV3.getPacket(client.sock)
        client2.publish(self.topics[0], b"qos 2", qos = 2)
        # Server -> Client: PUBLISH
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.PUBLISH
        # Client -> Server: PUBREC
        pubrec = MQTTV3.Pubrecs()
        pubrec.messageIdentifier = packet.messageIdentifier
        client.sock.send(pubrec.pack())
        # Server -> Client: PUBREL
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.PUBREL
        # Client -> Server: PUBCOMP
        pubcomp = MQTTV3.Pubcomps()
        pubcomp.messageIdentifier = packet.messageIdentifier
        client.sock.send(pubcomp.pack())

        # [MQTT-3.3.5-1]
        callback = Callbacks()
        client = mqtt_client.Client(clientid3, callback)
        client.connect(host=host, port=port, cleansession=True)
        client.subscribe([self.wildtopics[0]], [0])
        client.subscribe([self.topics[1]], [2])
        time.sleep(.5)
        client.publish(self.topics[1], b"test actions", qos = 1)
        waitfor(callback.messages, 2, 1)
        assert 0 in [callback.messages[0][2], callback.messages[1][2]]
        assert 1 in [callback.messages[0][2], callback.messages[1][2]]



