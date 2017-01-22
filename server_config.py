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
	def __init__(self, port):
		self.ip = gethostbyname(gethostname())
		self.port = port
		self.threads = []
		logging.info('Server running at {0:s}:{1:4d}'.format(self.ip, self.port))
		try:
			self.server = socket(AF_INET, SOCK_STREAM)
			self.server.bind((self.ip, self.port))
			self.server.listen(5) # Max connections
			logging.info('Server start successfully...')
		except Exception as e:
			logging.warning("Socket create fail.{0}".format(e)) #socket create fail

		self.dc = datacenter(port)
		self.conn_list = []
		self.rx_conn_list = []

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
		try:
			for center_id in self.dc.datacenters:
				if not int(center_id) == int(self.dc.datacenter_id):
					socketConnect = socket(AF_INET, SOCK_STREAM)
					socketConnect.connect((self.ip, int(center_id)))
					self.rx_conn_list.append(socketConnect)
					logging.info("Initial connection success %s" %center_id)
		except Exception as e:
			socketConnect.close()
			logging.error('Initial connection fail! {0}'.format(e))


	def server_process(self):
		num = len(self.dc.datacenters) - int(self.port%len(self.dc.datacenters))
		Timer(num*10, self.server_connect, ()).start()



	'''
		Handle reply or release 
	'''
	def server_reply(self):
		while True:
		 	if (len(self.rx_conn_list) == len(self.dc.datacenters)-1):
		 		for conn in self.rx_conn_list:
		 			data = conn.recv(1024)
				 	if data != '':
				 		print data
				 		# Problem data can only be string
				 		# if data type == ticket request
				 		# self.dc.handle_coordinate_request()
				 		# if data type == reply
				 		# self.dc.handle_coordinate_reply()
				 		# if data.type == release
				 		# self.dc.handle_coordinate_release()
				 		#	sell_ticket(ticket_request)




	def waitConnection(self):
		num = 0
		while True:
			try:
				conn, addr = self.server.accept()
				num += 1
				print num
				if num < len(self.dc.datacenters):
					self.conn_list.append(conn)
					conn.send("hallo")
					print("ha")
					print conn
				else:
					'''
						broadcast request
					'''
					#	ticket_request
					#	handle ticket_request
					logging.info('Connection from {address} connected!'.format(address=addr))
					msg = int(conn.recv(1024))
					tk_request = ticket_request(conn, msg)
					self.dc.handle_ticket_request(tk_request, self.conn_list)
					logging.info('Ask for {ticket_count} tickets!'.format(ticket_count=msg))
					conn.send("Thank you for connecting")
					time.sleep(5)
					conn.close()
			except Exception as e:
				logging.error('connect to with client error.{0}'.format(e))





# initialize the log
def initLog(logName):
	# createa log folder
	if not os.path.isdir('log'):
		os.mkdir('log')
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M', filename='log/' + logName)
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	formatter = logging.Formatter('%(levelname)-6s:%(message)s')
	console.setFormatter(formatter)
	logging.getLogger('').addHandler(console)





if __name__ == "__main__":
	initLog('server.log')
	#agrv[1] should be in the CONIG file
	server = Server(int(sys.argv[1]))


