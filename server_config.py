'''
	This is the server configuration
	Parameter: # of datacenters
	Implemented by Zhijing@Jan 20, 2017
'''
from multiprocessing import Process
import sys
import socket
import numpy as np
import datetime
import time



def connection(host, port_num):
	s = socket.socket()
	host = socket.gethostname()
	s.bind((host, port_num))
	s.listen(5)
	while(1):
		c, addr = s.accept()
		time.sleep(5)
		buy_ticket_num = int(c.recv(1024))
		upload_time = int(time.time()) #second level
		
		'''
			Xinyi!!!
			Lamport's Distributed Solution
			Input: upload_time, buy_ticket_num, port_num(or datacenter_id)
			Suppose the output is state (1: success; 0: unsuccess)
		'''

		state = 1

		'''
			Log information
		'''
		print("Current time is %s. Got connection from %s, ask for %s tickets at Server %s. The state is %s.\n" %(datetime.datetime.now(), addr, buy_ticket_num, port_num, state))

		if int(state) == 1:
			c.send("Successful! Thank you for connecting " + str(port_num))
		else:
			c.send("Sorry not enough tickets are left")

		c.close() 




def main():
	server_num = int(raw_input("How many servers do you want? "))
	total_ticket_num = int(raw_input("How many total tickets do you want? "))
	s = socket.socket() 
	host = socket.gethostname()
	port_list = list(np.array(range(server_num)) + 12345)
	'''
		Print out the server id
	'''
	port_file = open('port_list.txt','w')
	for port in port_list:
		port_file.write("%s\n" %port)

	for port in port_list:
		p = Process(target=connection, args=(host, port))
		p.start()
		


if __name__ == "__main__":
    main()
