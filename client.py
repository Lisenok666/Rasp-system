import http.client
import socket
import sys
import json
import logging
from time import sleep

class Client():
    def __init__(self):
        self.conn = None

    def connect(self, ip):
        self.conn = http.client.HTTPConnection(host=ip, port='8080')

    def close_connection(self):
        self.conn.close()
        self.conn = None

    def create_var(self, key, value):
        self.conn.request("POST", "", json.dumps({key: value}))
        response = self.conn.getresponse()
        print(response.status, response.reason)
        data = response.read()
        print(data)

    def update_var(self):
        pass

    def read_var(self, key):
        self.conn.request("GET", f"/db/{key}")
        response = self.conn.getresponse()
        print(response.status, response.reason)
        print(response.read())

    def delete_var(self, key):
        self.conn.request("DELETE", f"/db/{key}")
        response = self.conn.getresponse()
        print(response.status, response.reason)
        print(response.read())

    def find(self, role):
        msg = f"are you {role}?"
        ip = "172.17.255.255" # Docker's network 

        get_answer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        get_answer_sock.bind(("0.0.0.0", 5006))

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((ip,0))
        sock.sendto(msg.encode(), (ip, 5005))
        sock.close()

        data, addr = get_answer_sock.recvfrom(1024)
        get_answer_sock.close()
        logging.info("recieve {} from {}".format(data, addr))
        if data.decode() == f"i am {role}":
            logging.info(f"{role} found")
            return addr[0] #ip

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    c = Client()
    master_ip = c.find("master")
    c.connect(ip=master_ip)
    c.create_var(key="key1", value=5)
    c.create_var(key="key2", value=15)
    c.create_var(key="key3", value=25)
    c.close_connection()
    for i in range(1):
        replica_ip = c.find("replica")
        c.connect(ip=replica_ip)
        c.read_var("key1")
        c.read_var("key2")
        c.read_var("key3")
        c.read_var("key4")
        c.close_connection()
        logging.info(f"{i}: relica {replica_ip}")