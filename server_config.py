'''
    Interface of the Ticket Selling System
    Implemented by Zhijing@Jan 21, 2017
    Modified by Xinyi@Jan 22, 2017
'''

import shutil
import os, sys
from socket import *
import logging
import pickle
from threading import *
import threading
from thread import *
import datetime
import time
from datacenter import datacenter, ticket_request
from multiprocessing import Process, Manager
import json

CONFIG = json.load(open('config.json'))



class Server():
    def __init__(self, port, delay):
        self.ip = gethostbyname(gethostname())
        self.port = port
        self.threads = []
        self.delay = delay

        logging.info('Server running at {0:s}:{1:4d}'.format(self.ip, self.port))
        try:
            self.server = socket(AF_INET, SOCK_STREAM)
            self.server.bind((self.ip, self.port))
            self.server.listen(5) # Max connections
            logging.info('Server start successfully...')
        except Exception as e:
            logging.warning("Socket create fail.{0}".format(e)) #socket create fail

        # using port as datacenter id is a bad idea, change it if we have time: #No Time!#
        self.dc = datacenter(port, self)

        # store incomming connection from other datacenters, this is used for sending messages
        self.conn_list = {}
        # store outgoing connecion to other datacenters, this is used for receiving messages
        self.rx_conn_list = {}

        time.sleep(3)
        self.process()

    def process(self):
        #save the connection between each server pair#
        thread = threading.Thread(target=self.server_process(), args=())
        thread.start()

        #handle the reply#
        start_new_thread(self.server_reply, ())

        #handle request from client#
        self.waitConnection()

    def server_connect(self):
        ''' Initiating connection to peer servers, used for receiving future messages
        The established connections are stored in self.rx_conn_list'''
        try:
            for center_id in self.dc.datacenters:
                if not center_id == self.dc.datacenter_id:
                    socketConnect = socket(AF_INET, SOCK_STREAM)
                    socketConnect.connect((self.ip,
                                           self.dc.datacenters[center_id]['port']))
                    # send initialization message, identify it's center_id
                    socketConnect.send('INIT:{center_id}\n'.format(
                        center_id=self.dc.datacenter_id))
                    self.rx_conn_list[center_id] = socketConnect
                    logging.info("Initial connection success %s" %center_id)
        except Exception as e:
            socketConnect.close()
            logging.error('Initial connection fail! %s', e)


    def server_process(self):
        '''
        start the communication with server after a certain wait period,
        for testing different starting time
        '''
        num = len(self.dc.datacenters) - int(self.port%len(self.dc.datacenters))
        Timer(num*10, self.server_connect, ()).start()

    def broadcast_message(self, message):
        ''' broadcast the message to all datacenters '''
        time.sleep(self.delay)
        for conn in self.conn_list.values():
            print conn
            conn.send(message)

        logging.info('broadcasted message: %s', message.strip())

    def send_message(self, target_center_id, message):
        ''' send the message to a certain datacenter, identified by target_center_id '''
        time.sleep(self.delay)
        self.conn_list[target_center_id].send(message)
        logging.info('sent message to %s: %s', target_center_id, message.strip())

    def single_server_reply(self, center_id, conn):
        ''' taget single peer server, loop until request complete '''
        while True:
            data = conn.recv(1024)
            if data != '':
                logging.info('received message from %s: %s', center_id, data.strip())

                # call a general function for message parsing
                self.dc.handle_request(center_id, data)

    def server_reply(self):
        '''
        Handle incomming messages from peer datacenters
        '''
        while True:
            # wait until all connections are established
            if (len(self.rx_conn_list) == len(self.dc.datacenters)-1):
                logging.info('all peer connection established')
                for center_id in self.rx_conn_list:
                    conn = self.rx_conn_list[center_id]
                    start_new_thread(self.single_server_reply, (center_id, conn))
                break
            # time.sleep(1)


    def waitConnection(self):
        '''
        This function is used to wait for incomming connections,
        either from peer datacenters or from clients
        Incomming connections from other servers are stored in self.conn_list
        '''
        num = 0
        while True:
            try:
                conn, addr = self.server.accept()
                logging.info('Connection from {address} connected!'.format(address=addr))
                # decide whether the connection is from client or from server
                msg = conn.recv(1024)
                if msg.startswith('INIT:'):
                    center_id = int(msg.split(':')[1])
                    self.conn_list[center_id] = conn
                    logging.info('Incomming server is {center_id}!'.format(center_id=center_id))
                    logging.debug(conn)
                else:
                    '''
                    broadcast request
                    '''
                    #    ticket_request
                    #    handle ticket_request
                    # parse the message 
                    tk_request = ticket_request(conn, int(msg))
                    self.dc.handle_ticket_request(tk_request)
                    logging.info('Ask for {ticket_count} tickets!'.format(ticket_count=msg))
                    #conn.send("Thank you for connecting")
                    # time.sleep(5)
                    # should not close the connection, because the reply message is not sent yet
                    # conn.close()

            except Exception as e:
                logging.error('error with incomming connection. {0}'.format(e))


# initialize the log
def initLog(logName):
    ''' setup logging system '''
    # createa log folder
    if not os.path.isdir('log'):
        os.mkdir('log')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', 
                        datefmt='%m-%d %H:%M', filename='log/' + logName)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)-6s:%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


if __name__ == "__main__":
    initLog('server_%s.log' % sys.argv[1])
    #agrv[1] should be in the CONIG file
    delay = int(CONFIG['messageDelay'])
    server = Server(int(sys.argv[1]), delay)


