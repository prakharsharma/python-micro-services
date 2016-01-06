"""
Module provides generic utility methods
"""

import ConfigParser
import hashlib
import os
import subprocess
import time
import zmq


def redis_config_from_config_file(config, section, defaults=None):
    if defaults is None:
        defaults = {}
    ret = {}
    for x in ['host', 'port', 'db']:
        if x in defaults:
            ret[x] = defaults[x]

    if not config:
        return ret

    if not config.has_section(section):
        return ret

    if config.has_option(section, "host"):
        ret["host"] = config.get(section, "host")

    if config.has_option(section, "port"):
        ret["port"] = config.getint(section, "port")

    if config.has_option(section, "db"):
        ret["db"] = config.getint(section, "db")

    return ret


def zmq_socket_from_socket_type(context, socket_type):
    if socket_type == "REP":
        return context.socket(zmq.REP)
    elif socket_type == "REQ":
        return context.socket(zmq.REQ)
    elif socket_type == "PUB":
        return context.socket(zmq.PUB)
    elif socket_type == "SUB":
        return context.socket(zmq.SUB)
    elif socket_type == "PUSH":
        return context.socket(zmq.PUSH)
    elif socket_type == "PULL":
        return context.socket(zmq.PULL)


def enum(**enums):
    """
    A little trick that implements enum in Python
    :param enums: enum elements
    :return: the enum object
    """
    return type('Enum', (object,), enums)


def current_timestamp(seconds=False, milliseconds=False, microseconds=True):
    """
    Returns the current time as an int.
    The precision can be controlled to return the number of seconds,
    milliseconds, or microseconds.
    """
    now_ts = time.time()
    precision = 1000000
    if milliseconds:
        precision = 1000
    elif seconds:
        precision = 1
    return int(now_ts * precision)


def set_time_zone(timezone='America/Los_Angeles'):
    os.environ['TZ'] = timezone
    time.tzset()


def send_email(subject, body, to_address, from_address):
    try:
        fname = "/tmp/mail_content_%d" % int(time.time())
        f = open(fname, "w")
        f.write(body)
        f.close()
        subprocess.call(
            "mail -s '%s' -r '%s' %s < %s" %
            (subject, from_address, to_address, fname),
            shell=True
        )
        subprocess.call("rm -f %s" % fname, shell=True)
    except Exception as exception:
        raise exception


def get_hash_hexdigest(payload, algo='md5'):
    if isinstance(payload, list):
        payload = ', '.join([str(x) for x in payload])
    elif not isinstance(payload, str):
        raise RuntimeError('payload has to be either a string or list')
    hash_method = getattr(hashlib, algo, None)
    if hash_method is None:
        raise RuntimeError('algo: %s not available in hashlib module' % algo)
    return hash_method(payload).hexdigest()


# Be careful, this code will break in about 271 years
def normalize_timestamp_to_seconds(timestamp):
    timestamp_seconds_digits = 10
    return float(timestamp) / \
           (10 ** (len(str(int(timestamp))) - timestamp_seconds_digits))


def config_value(config, section, name, default):
    try:
        return config.get(section, name)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        return default


def decode_utf8_string(x):
    try:
        return x.decode('utf-8')
    except:
        return x


def ts_to_string(ts, time_format='%Y-%m-%d %H:%M:%S'):
    ts = normalize_timestamp_to_seconds(ts)
    return time.strftime(time_format, time.localtime(ts))


def string_to_ts(time_string, time_format='%Y-%m-%d %H:%M:%S',
                 milliseconds=False, microseconds=False):
    ts = time.mktime(time.strptime(time_string, time_format))
    if microseconds:
        return int(ts * 1000000)
    elif milliseconds:
        return int(ts * 1000)
    else:
        return int(ts)


def datetime_to_ts(datetime, microseconds=False, milliseconds=False):
    seconds = time.mktime(datetime.timetuple())
    if microseconds:
        return int(seconds * 1000000)
    elif milliseconds:
        return int(seconds * 1000)
    return int(seconds)


class Enum(object):

    def __init__(self, enum_names):
        for argi, enum_name in enumerate(enum_names):
            setattr(self, enum_name.upper(), argi)
        self._enum_names = map(lambda x: x.upper(), enum_names)

    def name_from_value(self, value):
        if value >= len(self._enum_names):
            raise ValueError('Invalid enum value')
        return self._enum_names[value]

    def value_from_name(self, name):
        if getattr(self, name.upper(), None) is None:
            raise ValueError('Invalid enum name')
        return getattr(self, name.upper())

    @property
    def names(self):
        return self._enum_names

