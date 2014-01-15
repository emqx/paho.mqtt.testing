"""
*******************************************************************
  Copyright (c) 2013, 2014 IBM Corp.
 
  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution. 
 
  The Eclipse Public License is available at 
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at 
    http://www.eclipse.org/org/documents/edl-v10.php.
 
  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

import traceback, random, sys, string, copy, threading, logging, socket, time

from ..formats import MQTTV311 as MQTTV3

from .Brokers import Brokers

def respond(sock, packet):
  logging.info("out: "+repr(packet))
  if hasattr(sock, "handlePacket"):
    sock.handlePacket(packet)
  else:
    sock.send(packet.pack())

class MQTTClients:

  def __init__(self, anId, cleansession, keepalive, socket, broker):
    self.id = anId # required
    self.cleansession = cleansession
    self.socket = socket
    self.msgid = 1
    self.outbound = [] # message objects - for ordering 
    self.outmsgs = {} # msgids to message objects
    self.broker = broker
    if broker.publish_on_pubrel:
      self.inbound = {} # stored inbound QoS 2 publications
    else:
      self.inbound = []
    self.connected = False
    self.will = None
    self.keepalive = keepalive
    self.lastPacket = None

  def reinit(self):
    self.__init__(self.id, self.socket)

  def resend(self):
    for pub in self.outbound:
      logging.debug("resending", pub)
      pub.fh.DUP = 1
      if pub.fh.QoS == 1:
        respond(self.socket, pub)
      elif pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          respond(self.socket, pub)
        else:
          resp = MQTTV3.Pubrels()
          resp.fh.DUP = 1
          resp.messageIdentifier = pub.messageIdentifier
          respond(self.socket, resp)

  def publishArrived(self, topic, msg, qos, retained=False):
    pub = MQTTV3.Publishes()
    pub.topicName = topic
    pub.data = msg
    pub.fh.QoS = qos
    pub.fh.RETAIN = retained
    if qos == 2:
      pub.qos2state = "PUBREC"
    if qos in [1, 2]:
      pub.messageIdentifier = self.msgid
      if self.msgid == 65535:
        self.msgid = 1
      else:
        self.msgid += 1
      self.outbound.append(pub)
      self.outmsgs[pub.messageIdentifier] = pub
    if qos == 0 and not self.broker.dropQoS0:
      self.outbound.append(pub)
    if self.connected:
      respond(self.socket, pub)
    else:
      print(self.id, "publishArrived", self.outbound)

  def puback(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 1:
        self.outbound.remove(pub)
        del self.outmsgs[msgid]
      else:
        logging.error("Puback received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      logging.error("Puback received for msgid %d, but no message found", msgid)

  def pubrec(self, msgid):
    rc = False
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          pub.qos2state = "PUBCOMP"
          rc = True
        else:
          logging.error("Pubrec received for msgid %d, but message in wrong state", msgid)
      else:
        logging.error("Pubrec received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      logging.error("Pubrec received for msgid %d, but no message found", msgid)
    return rc

  def pubcomp(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBCOMP":
          self.outbound.remove(pub)
          del self.outmsgs[msgid]
        else:
          logging.error("Pubcomp received for msgid %d, but message in wrong state", msgid)
      else:
        logging.error("Pubcomp received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:  
      logging.error("Pubcomp received for msgid %d, but no message found", msgid)

  def pubrel(self, msgid):
    rc = None
    if self.broker.publish_on_pubrel:
        pub = self.inbound[msgid]
        if pub.fh.QoS == 2:
          rc = pub
        else:
          logging.error("Pubrec received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      rc = msgid in self.inbound
    if not rc:
      logging.error("Pubrec received for msgid %d, but no message found", msgid)
    return rc 
  

class MQTTBrokers:

  def __init__(self, publish_on_pubrel=True, overlapping_single=True, dropQoS0=True):

    # optional behaviour
    self.publish_on_pubrel = publish_on_pubrel
    self.dropQoS0 = dropQoS0                    # don't queue QoS 0 messages for disconnected clients

    self.broker = Brokers(overlapping_single)
    self.clientids = {}
    self.clients = {}
    self.lock = threading.RLock()

    logging.info("MQTT 3.1.1 Paho Test Broker")
    logging.info("Optional behaviour, publish on pubrel: %s", self.publish_on_pubrel)
    logging.info("Optional behaviour, single publish on overlapping topics: %s", self.broker.overlapping_single)
    logging.info("Optional behaviour, drop QoS 0 publications to disconnected clients: %s", self.dropQoS0)

  def handleRequest(self, sock):
    "this is going to be called from multiple threads, so synchronize"
    self.lock.acquire()
    terminate = False
    try:
      raw_packet = MQTTV3.getPacket(sock)
      if raw_packet == None:
        # will message
        self.disconnect(sock, None, terminate=True)
        logging.info("[MQTT-3.1.2-8] sending will message")
        terminate = True
      else:
        packet = MQTTV3.unpackPacket(raw_packet)
        if packet:
          terminate = self.handlePacket(packet, sock)
        else:
          raise MQTTV3.MQTTException("[MQTT-2.0.0-1] handleRequest: badly formed MQTT packet")
    finally:
      self.lock.release()
    return terminate

  def handlePacket(self, packet, sock):
    terminate = False
    logging.info("in: "+repr(packet))
    if sock not in self.clientids.keys() and \
         MQTTV3.packetNames[packet.fh.MessageType] != "CONNECT":
      self.disconnect(sock, packet)
      raise MQTTV3.MQTTException("[MQTT-3.1.0-1] Connect was not first packet on socket")
    else:
      getattr(self, MQTTV3.packetNames[packet.fh.MessageType].lower())(sock, packet)
      if sock in self.clients.keys():
        self.clients[sock].lastPacket = time.time()
    if MQTTV3.packetNames[packet.fh.MessageType] == "DISCONNECT":
      terminate = True
    return terminate

  def connect(self, sock, packet):
    if packet.ProtocolName != "MQTT":
      self.disconnect(sock, None)
      raise MQTTV3.MQTTException("[MQTT-3.1.2-1] Wrong protocol name %s" % packet.ProtocolName)
    if packet.ProtocolVersion != 4:
      logging.error("[MQTT-3.1.2-2] Wrong protocol version %d", packet.ProtocolVersion)
      resp = MQTTV3.Connacks()
      resp.returnCode = 1
      respond(sock, resp)
      self.disconnect(sock, None)
      return
    if packet.ClientIdentifier in self.clientids.values():
      for s in self.clientids.keys():
        if self.clientids[s] == packet.ClientIdentifier:
          if s == sock:
            self.disconnect(sock, None)
            raise MQTTV3.MQTTException("[MQTT-3.1.0-2] Second connect packet")
          else:
            logging.info("[MQTT-3.1.4-2] Disconnecting old client %s", packet.ClientIdentifier)
            self.disconnect(s, None)
            break
    self.clientids[sock] = packet.ClientIdentifier
    me = None
    if not packet.CleanSession:
      me = self.broker.getClient(packet.ClientIdentifier) # find existing state, if there is any
    if me == None:
      me = MQTTClients(packet.ClientIdentifier, packet.CleanSession, packet.KeepAliveTimer, sock, self)
    else: 
      me.socket = sock # set existing client state to new socket
      me.cleansession = packet.CleanSession
      me.keepalive = packet.KeepAliveTimer
    self.clients[sock] = me
    me.will = (packet.WillTopic, packet.WillQoS, packet.WillMessage, packet.WillRETAIN) if packet.WillFlag else None
    self.broker.connect(me)
    resp = MQTTV3.Connacks()
    resp.returnCode = 0
    respond(sock, resp)
    me.resend()

  def disconnect(self, sock, packet, terminate=False):
    if sock in self.clientids.keys():
      if terminate:
        self.broker.terminate(self.clientids[sock])
      else:
        self.broker.disconnect(self.clientids[sock])
      del self.clientids[sock]
    if sock in self.clients.keys():
      del self.clients[sock]
    sock.shutdown(socket.SHUT_RDWR) # must call shutdown to close socket immediately
    sock.close()

  def disconnectAll(self, sock):
    for sock in self.clientids.keys():
      self.disconnect(sock, None)

  def subscribe(self, sock, packet):
    topics = []
    qoss = []
    for p in packet.data:
      topics.append(p[0])
      qoss.append(p[1])
    self.broker.subscribe(self.clientids[sock], topics, qoss)
    resp = MQTTV3.Subacks()
    resp.messageIdentifier = packet.messageIdentifier
    resp.data = qoss
    respond(sock, resp)

  def unsubscribe(self, sock, packet):
    self.broker.unsubscribe(self.clientids[sock], packet.data)
    resp = MQTTV3.Unsubacks()
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def publish(self, sock, packet):
    if packet.topicName.find("+") != -1 or packet.topicName.find("#") != -1:
      raise MqttException("[MQTT-3.3.2-2] wildcards not allowed in topic name")
    if packet.fh.QoS == 0:
      self.broker.publish(self.clientids[sock],
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
    elif packet.fh.QoS == 1:
      self.broker.publish(self.clientids[sock],
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubacks()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)
    elif packet.fh.QoS == 2:
      myclient = self.clients[sock]
      if self.publish_on_pubrel:
        if packet.messageIdentifier in myclient.inbound.keys():
          if packet.fh.DUP == 0:
            logging.error("[MQTT-2.1.2-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logging.info("[MQTT-2.1.2-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound[packet.messageIdentifier] = packet
      else:
        if packet.messageIdentifier in myclient.inbound:
          if packet.fh.DUP == 0:
            logging.error("[MQTT-2.1.2-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logging.info("[MQTT-2.1.2-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound.append(packet.messageIdentifier)
          self.broker.publish(myclient, packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubrecs()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubrel(self, sock, packet):
    myclient = self.clients[sock]
    pub = myclient.pubrel(packet.messageIdentifier)
    if pub:
      if self.publish_on_pubrel:
        self.broker.publish(myclient.id, pub.topicName, pub.data, pub.fh.QoS, pub.fh.RETAIN)
        del myclient.inbound[packet.messageIdentifier]
      else:
        myclient.inbound.remove(packet.messageIdentifier)
    # must respond with pubcomp regardless of whether a message was found
    if not pub:
      logging.info("[MQTT-3.6.4-1] must respond with a pubcomp packet")
    resp = MQTTV3.Pubcomps()
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def pingreq(self, sock, packet):
    resp = MQTTV3.Pingresps()
    respond(sock, resp)

  def puback(self, sock, packet):
    "confirmed reception of qos 1"
    self.clients[sock].puback(packet.messageIdentifier)

  def pubrec(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    if myclient.pubrec(packet.messageIdentifier):
      resp = MQTTV3.Pubrels()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubcomp(self, sock, packet):
    "confirmed reception of qos 2"
    self.clients[sock].pubcomp(packet.messageIdentifier)

  def keepalive(self, sock):
    client = self.clients[sock]
    if client.keepalive > 0 and time.time() - client.lastPacket > client.keepalive * 1.5:
      # keep alive timeout
      logging.info("[MQTT-3.1.2-22] keepalive timeout for client %s", client.id)
      self.disconnect(sock, None, terminate=True)





