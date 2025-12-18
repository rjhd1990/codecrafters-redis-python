import argparse
import socket  # noqa: F401
import asyncio
import threading

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

async def handle_connection(reader, writer):
    """
    This function is called for each new client connection.
    """
    # Gte the client's address for logging
    client_addr = writer.get_extra_info("peername")
    print(f"✅ New connection from {client_addr}")
    try:
        while True:
            data = await asyncio.wait_for(reader.read(1024), timeout=60.0)
            if not data:
                print(f"⭕️ Client {client_addr} disconnected")
                break
            message = data.decode()
            parsed = parse_rep(message)
            print(f"➡️ Received '{parsed}' from {client_addr}, {in_memory_store}")
            if not parsed:
                continue
            command = parsed[0].upper()
            if command == "PING":
                writer.write(b"+PONG\r\n")
            elif command == "ECHO":
                arg = parsed[1]
                response = bulk_string(arg)
                writer.write(response)
            elif command == "SET":
                in_memory_store[parsed[1]] = parsed[2]
                if len(parsed) >= 4 and parsed[3].lower() in ["px"]:
                    ttl =int(parsed[4])
                    print("ttl",ttl)
                    threading.Timer(ttl/1000, in_memory_store.pop, args=[parsed[1]]).start()
                
                writer.write(simple_string("OK"))
            elif command == "GET":
                value = in_memory_store.get(parsed[1])
                if value is None:
                    writer.write("$-1\r\n".encode())    
                else:
                    writer.write(bulk_string(value))
            elif command == "RPUSH":
                key = parsed[1]
                values = parsed[2:]
                in_memory_store.setdefault(key, [])
                in_memory_store[key].extend(values)
                writer.write(f":{len(values)}\r\n".encode())
            else:
                writer.write("$-1\r\n".encode())
            #send the data immediately
            await writer.drain()
    except asyncio.TimeoutError:
        print(f"🕰️ Connection with {client_addr} timed out.")
    except ConnectionResetError:
        print(f"❌ Connection reset by {client_addr}.")
    except Exception as e:
        print(f"An unexpected error occurred with {client_addr}: {e}")
    finally:
        # No matter what happens, always close the connection
        print(f"Closing connection with {client_addr}")
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
    print("🚀 Launching server on...")
    print(f"   Host: {args.host}")
    print(f"   Port: {args.port}")

    server = await asyncio.start_server(handle_connection, args.host, args.port)

    print(f"🚀 Server listening on {args.host}:{args.port}")

    async with server:
        await server.serve_forever()



if __name__ == "__main__":
    asyncio.run(main())
