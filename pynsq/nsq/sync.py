import socket
import struct

import nsq


class SyncConn(object):
    def __init__(self, timeout=1.0):
        self.buffer = ''
        self.timeout = timeout
    
    def connect(self, host, port):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(self.timeout)
        self.s.connect((host, port))
        self.s.send(nsq.MAGIC_V2)
    
    def _readn(self, size):
        while True:
            if len(self.buffer) >= size:
                break
            packet = self.s.recv(4096)
            if not packet:
                raise Exception("failed to read %d" % size)
            self.buffer += packet
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data
    
    def read_response(self):
        size = struct.unpack('>l', self._readn(4))[0]
        return self._readn(size)
    
    def send(self, data):
        self.s.send(data)


if __name__ == '__main__':
    c = SyncConn()
    c.connect("127.0.0.1", 4150)
    c.send(nsq.subscribe('test', 'ch', 'a', 'b'))
    for i in xrange(10):
        c.send(nsq.ready(1))
        resp = c.read_response()
        unpacked = nsq.unpack_response(resp)
        msg = nsq.decode_message(unpacked[1])
        print msg.id, msg.body
        c.send(nsq.finish(msg.id))
