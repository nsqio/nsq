import socket
import struct
import logging

import tornado.iostream

import nsq


class AsyncConn(object):
    def __init__(self, host, port, connect_callback, data_callback, close_callback, timeout=1.0):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        assert callable(connect_callback)
        assert callable(data_callback)
        assert callable(close_callback)
        assert isinstance(timeout, float)
        
        self.connecting = False
        self.connected = False
        self.host = host
        self.port = port
        self.connect_callback = connect_callback
        self.data_callback = data_callback
        self.close_callback = close_callback
        self.timeout = timeout
    
    def connect(self):
        if self.connected or self.connecting:
            return
        
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(self.timeout)
        self.s.setblocking(0)
        
        self.stream = tornado.iostream.IOStream(self.s)
        self.stream.set_close_callback(self._socket_close)
        
        self.connecting = True
        self.stream.connect((self.host, self.port), self._connect_callback)
    
    def _connect_callback(self):
        self.connecting = False
        self.connected = True
        self.stream.write(nsq.MAGIC_V2)
        self._start_read()
        try:
            self.connect_callback(self)
        except Exception:
            logging.exception("uncaught exception in connect_callback")
    
    def _start_read(self):
        self.stream.read_bytes(4, self._read_size)
    
    def _socket_close(self):
        self.connected = False
        try:
            self.close_callback(self)
        except Exception:
            logging.exception("uncaught exception in close_callback")
    
    def close(self):
        self.connected = False
        self.stream.close()
    
    def _read_size(self, data):
        try:
            size = struct.unpack('>l', data)[0]
            self.stream.read_bytes(size, self._read_body)
        except Exception:
            self.close()
            logging.exception("failed to unpack size")
    
    def _read_body(self, data):
        try:
            self.data_callback(self, data)
        except Exception:
            logging.exception("uncaught exception in data_callback")
        tornado.ioloop.IOLoop.instance().add_callback(self._start_read)
    
    def send(self, data):
        self.stream.write(data)
    
    def __str__(self):
        return self.host + ':' + str(self.port)


if __name__ == '__main__':
    def connect_callback(c):
        print "connected"
        c.send(nsq.subscribe('test', 'ch', 'a', 'b'))
        c.send(nsq.ready(1))
    
    def close_callback(c):
        print "connection closed"
    
    def data_callback(c, data):
        unpacked = nsq.unpack_response(data)
        if unpacked[0] == nsq.FRAME_TYPE_MESSAGE:
            c.send(nsq.ready(1))
            msg = nsq.decode_message(unpacked[1])
            print msg.id, msg.body
            c.send(nsq.finish(msg.id))
    
    c = AsyncConn("127.0.0.1", 4150, connect_callback, data_callback, close_callback)
    c.connect()
    
    tornado.ioloop.IOLoop.instance().start()
