import asyncio
import time
import threading
from . import logging
from .resp_parser import RESP_Encoder
from .stream_items import StreamHandler, StreamData

in_memory_store = {}


def get_values(parsed_arg):
    key = parsed_arg[1]
    values = in_memory_store.get(key, [])
    return values


def lrange_command(parsed_arg):
    values = get_values(parsed_arg)
    start = int(parsed_arg[2])
    start = max(len(values) + start, 0) if start < 0 else start
    stop = min(int(parsed_arg[3]), len(values) - 1)
    stop = max(len(values) + stop, 0) if stop < 0 else stop
    if len(values) == 0 or start >= len(values) or start > stop:
        return "*0\r\n".encode()
    else:
        return RESP_Encoder.array_string(values[start : stop + 1])


def lpop_command(values, n=0):
    if len(values) <= 0:
        return "$-1\r\n".encode()
    else:
        if n > 0:
            pop_vs = []
            for _ in range(n):
                pop_vs.append(values.pop(0))
            response = RESP_Encoder.array_string(pop_vs)
        else:
            old_v = values.pop(0)
            response = RESP_Encoder.bulk_string(old_v)
    return response


async def blpop_command(parsed_arg):
    size = 0
    timeout = float(parsed_arg[-1])
    start_timer = time.time()
    item = None
    key = parsed_arg[1]
    while size == 0:
        values = in_memory_store.get(key, [])
        size = len(values)
        if len(values) > 0:
            item = values.pop(0)
            break
        if timeout > 0 and (time.time() - start_timer >= timeout):
            break
        await asyncio.sleep(0.1)  # smaller delay before retry
    if item is None:
        return "*-1\r\n".encode()
    return RESP_Encoder.array_string([key, item])


def push_command(push_type, parsed_arg):
    key = parsed_arg[1]
    values = parsed_arg[2:] or []
    in_memory_store.setdefault(key, [])
    if push_type == "RPUSH":
        in_memory_store[key].extend(values)
    elif push_type == "LPUSH":
        values.reverse()
        in_memory_store[key][:0] = values
    return


def set_command(parsed_arg):
    in_memory_store[parsed_arg[1]] = parsed_arg[2]
    if len(parsed_arg) >= 4 and parsed_arg[3].lower() in ["px"]:
        ttl = int(parsed_arg[4])
        logging.info(f"ttl: {ttl}")
        threading.Timer(ttl / 1000, in_memory_store.pop, args=[parsed_arg[1]]).start()


def type_command(key):
    value = in_memory_store.get(key)
    if value is None:
        return RESP_Encoder.simple_string("none")
    if isinstance(value, str):
        return RESP_Encoder.simple_string("string")
    elif isinstance(value, list):
        return RESP_Encoder.simple_string("list")
    elif isinstance(value, set):
        return RESP_Encoder.simple_string("set")
    elif isinstance(value, StreamData):
        return RESP_Encoder.simple_string("stream")


def stream_xadd_command(key, parsed_arg):
    sid = parsed_arg[2]
    sdata = in_memory_store.get(key)
    sObj = StreamHandler(sdata)
    items = []
    for a in range(3, len(parsed_arg), 2):
        items.append((parsed_arg[a], parsed_arg[a + 1]))
    success, rsp = sObj.xadd(sid, items)
    if success:
        in_memory_store[key] = sObj.seralized_data()
        return RESP_Encoder.bulk_string(rsp)
    else:
        return RESP_Encoder.error_string(rsp)


def stream_xrange_command(key, parsed_arg):
    sdata = in_memory_store.get(key)
    sObj = StreamHandler(sdata)
    start_id = parsed_arg[2]
    end_id = parsed_arg[3]
    resp = sObj.search(start_id, end_id)
    return RESP_Encoder.array_string(resp)

def stream_xread_command(parsed_arg):
    key = parsed_arg[2]
    sid = parsed_arg[3]
    sdata = in_memory_store.get(key)
    sObj = StreamHandler(sdata)
    resp = sObj.search(sid, "+")
    return RESP_Encoder.array_string([[ key, resp]])
    

class CommandHandler:
    @staticmethod
    async def run_command(parsed_arg, writer):
        key = None
        if len(parsed_arg) > 1:
            key = parsed_arg[1]
        command = parsed_arg[0].upper()
        if command == "PING":
            writer.write(b"+PONG\r\n")
        elif command == "ECHO":
            response = RESP_Encoder.bulk_string(key)
            writer.write(response)
        elif command == "SET":
            set_command(parsed_arg)
            writer.write(RESP_Encoder.simple_string("OK"))
        elif command == "GET":
            value = in_memory_store.get(key)
            if value is None:
                writer.write("$-1\r\n".encode())
            else:
                writer.write(RESP_Encoder.bulk_string(value))
        elif command == "RPUSH":
            push_command("RPUSH", parsed_arg)
            writer.write(f":{len(in_memory_store[key])}\r\n".encode())
        elif command == "LPUSH":
            push_command("LPUSH", parsed_arg)
            writer.write(f":{len(in_memory_store[key])}\r\n".encode())
        elif command == "LRANGE":
            writer.write(lrange_command(parsed_arg))
        elif command == "LLEN":
            values = get_values(parsed_arg)
            writer.write(f":{len(values)}\r\n".encode())
        elif command == "LPOP":
            values = get_values(parsed_arg)
            n = int(parsed_arg[2]) if len(parsed_arg) > 2 else 0
            writer.write(lpop_command(values, n))
        elif command == "BLPOP":
            response = await blpop_command(parsed_arg)
            writer.write(response)
        elif command == "TYPE":
            response = type_command(key)
            writer.write(response)
        elif command == "XADD":
            response = stream_xadd_command(key, parsed_arg)
            writer.write(response)
        elif command == "XRANGE":
            response = stream_xrange_command(key, parsed_arg)
            writer.write(response)
        elif command == "XREAD":
            response = stream_xread_command(parsed_arg)
            writer.write(response)
        else:
            writer.write("$-1\r\n".encode())
        # send the data immediately
        await writer.drain()
