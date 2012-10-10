import struct
import re

MAGIC_V2 = "  V2"
NL = "\n"

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2


class Message(object):
    def __init__(self, id, body, timestamp, attempts):
        self.id = id
        self.body = body
        self.timestamp = timestamp
        self.attempts = attempts


def unpack_response(data):
    frame = struct.unpack('>l', data[:4])[0]
    return frame, data[4:]

def decode_message(data):
    timestamp = struct.unpack('>q', data[:8])[0]
    attempts = struct.unpack('>h', data[8:10])[0]
    id = data[10:26]
    body = data[26:]
    return Message(id, body, timestamp, attempts)

def _command(cmd, *params):
    return "%s %s%s" % (cmd, ' '.join(params), NL)

def subscribe(topic, channel, short_id, long_id):
    assert valid_topic_name(topic)
    assert valid_channel_name(channel)
    return _command('SUB', topic, channel, short_id, long_id)

def ready(count):
    return _command('RDY', str(count))

def finish(id):
    return _command('FIN', id)

def requeue(id, time_ms):
    return _command('REQ', id, time_ms)

def nop():
    return _command('NOP')

def valid_topic_name(topic):
    if re.match(r'^[\.a-zA-Z0-9_-]+$', topic):
        return True
    return False

def valid_channel_name(channel):
    if re.match(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$', channel):
        return True
    return False
