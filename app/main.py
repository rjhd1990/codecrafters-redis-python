import argparse
import socket  # noqa: F401
import asyncio
import time
import threading
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

in_memory_store = {}

def parse_rep(message):
    parts = message.split("\r\n")
    if not parts[0].startswith("*"):
        return []
    num_elements = int(parts[0][1:])
    index =1
    result = []
    for i in range(num_elements):
        if index < len(parts) and parts[index].startswith("$"):
            index += 1
            result.append(parts[index])
            index += 1
    return result

def bulk_string(arg):
    return f"${len(arg)}\r\n{arg}\r\n".encode()

def simple_string(message):
    return f"+{message}\r\n".encode()

def array_string(array):
    ret = f"*{len(array)}\r\n"
    for item in array:
        ret += f"${len(item)}\r\n{item}\r\n"
    return ret.encode()

def get_values(parsed):
    key = parsed[1]
    values = in_memory_store.get(key, [])
    return values

def lrange_command(parsed):
    values = get_values(parsed)
    start = int(parsed[2])
    start = max(len(values)+start, 0) if start < 0 else start
    stop = min(int(parsed[3]), len(values) - 1)
    stop = max(len(values)+stop, 0) if stop <0 else stop
    if len(values) == 0 or start >= len(values) or start > stop:
        return "*0\r\n".encode()
    else:
        return array_string(values[start:stop+1])

def lpop_command(values, n=0):
    if len(values) <= 0:
        return "$-1\r\n".encode()
    else:
        if n > 0:
            pop_vs = []
            for _ in range(n):
                pop_vs.append(values.pop(0))
            response = array_string(pop_vs)
        else:
            old_v = values.pop(0)
            response = bulk_string(old_v)
    return response

async def blpop_command(parsed):
    size = 0
    timeout = float(parsed[-1])
    start_timer = time.time()
    item = None
    key = parsed[1]
    while size == 0:
        values = in_memory_store.get(key, [])
        size = len(values)
        if len(values) > 0:
            item = values.pop(0)
            break
        if timeout > 0 and (time.time()-start_timer >=timeout):
            break
        await asyncio.sleep(0.1) # smaller delay before retry
    if item is None:
        return "*-1\r\n".encode()
    return array_string([key, item])
            
def push_command(push_type, parsed):
    key = parsed[1]
    values = parsed[2:] or []
    in_memory_store.setdefault(key, [])
    if push_type == "RPUSH":
        in_memory_store[key].extend(values)
    elif push_type == "LPUSH":
        values.reverse()
        in_memory_store[key][:0] = values
    return 

def set_command(parsed):
    in_memory_store[parsed[1]] = parsed[2]
    if len(parsed) >= 4 and parsed[3].lower() in ["px"]:
        ttl =int(parsed[4])
        logging.info("ttl",ttl)
        threading.Timer(ttl/1000, in_memory_store.pop, args=[parsed[1]]).start()

async def handle_connection(reader, writer):
    """
    This function is called for each new client connection.
    """
    # Gte the client's address for logging
    client_addr = writer.get_extra_info("peername")
    logging.info(f"✅ New connection from {client_addr}")
    try:
        while True:
            data = await asyncio.wait_for(reader.read(1024), timeout=60.0)
            if not data:
                logging.info(f"⭕️ Client {client_addr} disconnected")
                break
            message = data.decode()
            parsed = parse_rep(message)
            logging.info(f"➡️ Received '{parsed}' from {client_addr}, {in_memory_store}")
            if not parsed:
                continue
            key = None
            if len(parsed) > 1:
                key = parsed[1]
            command = parsed[0].upper()
            if command == "PING":
                writer.write(b"+PONG\r\n")
            elif command == "ECHO":
                response = bulk_string(key)
                writer.write(response)
            elif command == "SET":
                set_command(parsed)
                writer.write(simple_string("OK"))
            elif command == "GET":
                key = parsed[1]
                value = in_memory_store.get(key)
                if value is None:
                    writer.write("$-1\r\n".encode())    
                else:
                    writer.write(bulk_string(value))
            elif command == "RPUSH":
                push_command("RPUSH", parsed)
                writer.write(f":{len(in_memory_store[key])}\r\n".encode())
            elif command == "LPUSH":
                push_command("LPUSH", parsed)
                writer.write(f":{len(in_memory_store[key])}\r\n".encode())
            elif command == "LRANGE":
                 writer.write(lrange_command(parsed))
            elif command == "LLEN":
                values = get_values(parsed)
                writer.write(f":{len(values)}\r\n".encode())
            elif command == "LPOP":
                values = get_values(parsed)
                n = int(parsed[2]) if len(parsed) > 2 else 0
                writer.write(lpop_command(values, n))
            elif command == "BLPOP":
                response = await blpop_command(parsed)
                writer.write(response)    
            else:
                writer.write("$-1\r\n".encode())
            #send the data immediately
            await writer.drain()
    except asyncio.TimeoutError:
        logging.error(f"🕰️ Connection with {client_addr} timed out.")
    except ConnectionResetError:
        logging.error(f"❌ Connection reset by {client_addr}.")
    except Exception as e:
        logging.exception(f"An unexpected error occurred with {client_addr}: {e}")
    finally:
        # No matter what happens, always close the connection
        logging.error(f"Closing connection with {client_addr}")
        writer.close()
        await writer.wait_closed()

async def main():
    parser = argparse.ArgumentParser(description="A smimple Redis-like server.")

    parser.add_argument(
        "-H",
        "--host",
        type=str,
        default="127.0.0.1",
        help="The interface to listen on (default: 127.0.0.1)."
    )

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=6379,
        help="The Port to listen on (default: 6380)."
    )

    args = parser.parse_args()
    logging.info("🚀 Launching server on...")
    logging.info(f"   Host: {args.host}")
    logging.info(f"   Port: {args.port}")

    server = await asyncio.start_server(handle_connection, args.host, args.port)

    logging.info(f"🚀 Server listening on {args.host}:{args.port}")

    async with server:
        await server.serve_forever()



if __name__ == "__main__":
    asyncio.run(main())
