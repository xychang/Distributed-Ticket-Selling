'''
    Interface of the Ticket Selling System
    Implemented by Zhijing@Jan 20, 2017
'''

import socket
import datetime
import sys
import time
import json


def display():
    port_list = []
    CONFIG = json.load(open('config.json'))
    port_list = map(str, [center['port'] for center in CONFIG['datacenters'].values()])
    choice = display_screen(port_list)
    return choice, port_list



def display_screen(port_list):
    print("\n\n*********************************\n\n")
    print("Welcome to SANDLAB Ticket Office!")
    print("The current time is " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("Ticket center list: " + str(port_list))
    print("\n\n*********************************")
    choice = raw_input("Wanna watch the game between UCSB and Cal Ploy? Press *Y*! ")
    while(choice != 'Y'):
        choice = display_screen(port_list)
    return choice



def client_server(port, ticket_num):
    c = socket.socket()
    host = socket.gethostname()
    c.connect((host, int(port)))
    c.sendall(ticket_num)
    time.sleep(2)
    print c.recv(1024)
    c.close()



def main():
    '''
        Display
    '''
    choice = 0
    while(choice != 'Y'):
        (choice, port_list) = display()
        choice_server = raw_input("Choose one of the datacenter from above or *N* to go back: ")
        if choice_server == 'N':
            choice = 'N'
        else:
            while(choice_server not in port_list):
                choice_server = raw_input("Enter valid datacenter ID or *N* to go back: ")
                if choice_server == 'N':
                    choice = 'N'
                    break
            else:
                ticket_num = raw_input("How many tickets do you want? ")
                print("OK. Processing...\n\n")
                client_server(choice_server, ticket_num)
                choice = 0
                time.sleep(3)




if __name__ == "__main__":
    main()