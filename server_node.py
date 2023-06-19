import http.server as hs
from threading import Thread, Lock
import socket
import sys
import os
import json
import signal
import time
from time import sleep
import logging

from storage_ops import *
from wal_ops import *

master = False
role = "replica"
local_ip = None
lock = Lock()

def get_ip():
    global local_ip
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()

class TestHandler(hs.BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        logging.info("GET RESPONSE %s", self.path)
        if self.path.startswith("/db"):
            self._set_response()
            key = self.path[4:]
            value = read(key)
            self.wfile.write(f"{value}".encode("utf-8"))

    def do_POST(self):
        global master
        if not master:
            return
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        post_data = json.loads(post_data)
        for key, value in post_data.items():
            create(key=key, value=value)
            break
        write_to_wal({"opcode": "CREATE", "key": key, "value": value})
        self._set_response()

    def do_PATCH(self):
        global master
        if not master:
            return
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        post_data = json.loads(post_data)
        for key, value in post_data.items():
            update(key=key, value=value)
            break
        write_to_wal({"opcode": "UPDATE", "key": key, "value": value})
        self._set_response()

    def do_DELETE(self):
        global master
        if not master:
            return
        if self.path.startswith("/db"):
            key = self.path[4:]
            if delete(key):
                self._set_response()
                write_to_wal({"opcode": "DELETE", "key": key})

class Worker():
    def __init__(self):

        init_storage()
        init_wal()

        self.server_process = Thread(target=self.run_server)
        self.replication_process = Thread(target=self.replication)
        self.broadcast_process = Thread(target=self.answer_broadcast)

        self.server_process.start()
        self.replication_process.start()
        self.broadcast_process.start()

    def run_server(self):
        server_address = ("", 8080)
        server = hs.HTTPServer(server_address, TestHandler)
        server.serve_forever()

    def replication(self):
        global local_ip
        
        last_replicated = 0
        ip = "172.17.255.255" # Docker's network 

        get_answer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        get_answer_sock.bind(("0.0.0.0", 5007))

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((ip,0))

        while True:
            last_change = last_from_wal()
            if last_replicated == last_change: 
                sleep(1)
                continue

            logging.info("Start replication")
            change = json.dumps(read_from_wal(last_replicated + 1))
            sock.sendto(f"REPLICA: {change}".encode(), (ip, 5005))
            success = 0
            max_time = time.time() + 1
            while True:
                if time.time() > max_time:
                    break
                get_answer_sock.settimeout(0.1)
                try:
                    data, addr = get_answer_sock.recvfrom(1024)
                except Exception:
                    continue                    
                get_answer_sock.settimeout(None)
                data = data.decode()
                data = json.loads(data)
                logging.info("recieve {} from {}".format(data, addr))
                if data["res"] == "success":
                    if data["idx"] == last_replicated + 1:
                        success += 1
                    continue
                if data["res"] == "needed":
                    begin = data["from"]
                    end = data["to"]
                    for i in range(begin, end + 1):
                        change = json.dumps(read_from_wal(i))
                        sock.sendto(f"REPLICA:{change}".encode(), (addr[0], 5005))
            logging.info("replicas send success")
            last_replicated += 1
            # get_answer_sock.close()

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

        get_answer_sock.settimeout(10)
        try:
            data, addr = get_answer_sock.recvfrom(1024)
        except Exception as e:
            if role == "master":
                self.became_candidate()
                # self.broadcast("let's vote for new master")
                # votes = {"me": 0, "not_me": 0}
                # while True:
                #     vote, addr = recvfrom(1024)
                #     if vote.decode == local_ip:
                #         votes["me"] += 1
                #     else:
                #         votes["not_me"] += 1
                # if votes["me"] > votes["not_me"]:
                #     self.became_master()
                #     self.broadcast("i am new master")


            return None
        if data.decode() == f"i am {role}":
            logging.info(f"{role} found")
        logging.info("recieve {} from {}".format(data, addr))
        get_answer_sock.close()
        return addr[0] # ip

    def became_master(self):
        global master
        logging.info("server is master")
        lock.acquire()
        master = True
        lock.release()

    def became_candidate(self):
        global candidate
        logging.info("server is candidate to master")
        lock.acquire()
        candidate = True
        lock.release()

    def is_master(self):
        global master
        result = False
        lock.acquire()
        result = master
        lock.release()
        return result
    
    def answer_broadcast(self):
        global master
        change_counter = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.bind(("0.0.0.0", 5005))
        while True:
            data, addr = sock.recvfrom(1024)
            data = data.decode()
            if addr[0] == local_ip:
                continue
            logging.info("recieve {} from {}".format(data, addr))
            if data == "are you master?":
                if self.is_master():
                    sock.sendto(b"i am master", (addr[0], 5006))
            elif data == "are you replica?":
                sock.sendto(b"i am replica", (addr[0], 5006))
            elif data.startswith("REPLICA:"):
                start = len("REPLICA:")
                data = json.loads(data[start:])
                counter = data["idx"]
                logging.info(f"{counter} / {change_counter}")
                if counter == change_counter + 1:
                    do_change(data)
                    change_counter += 1
                    answer = json.dumps({"res": "success", "idx": data["idx"]})
                    sock.sendto(f"{answer}".encode(), (addr[0], 5007)) 
                elif counter > change_counter + 1:
                    answer = json.dumps({"res": "needed", "from": change_counter + 1, "to": counter - 1})
                    sock.sendto(f"{answer}".encode(), (addr[0], 5007))
                    needed_changes = {change: None for change in range(change_counter + 1, counter)}
                    needed_changes[counter] = data
                    for _ in range(change_counter + 1, counter):
                        data, addr = sock.recvfrom(1024)
                        logging.info("needed recieve {} from {}".format(data, addr))
                        data = data.decode()
                        if data.startswith("REPLICA:"):
                            start = len("REPLICA:")
                            data = json.loads(data[start:])
                            idx = data["idx"]
                            needed_changes[idx] = data
                    for i in range(change_counter + 1, counter + 1):
                        do_change(needed_changes[i])
                        change_counter += 1
                    answer = json.dumps({"res": "success", "idx": data["idx"]})
                    sock.sendto(f"{answer}".encode(), (addr[0], 5007))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    get_ip()

    w = Worker()
    logging.info("server started")
    role = os.environ.get('ROLE', 'slave')

    if role == 'master':
        if w.find("master") is None:
            w.became_master()
        # else:
        # поменяться ролями с текущим мастером
    if not master:
        w.find("master")
    w.find("replica")
    while True:
        sleep(1)
