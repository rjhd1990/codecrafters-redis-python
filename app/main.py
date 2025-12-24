import argparse
import socket  # noqa: F401
import asyncio
from .resp_parser import parse_rep
from .command_handler import CommandHandler
from . import logging


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
            logging.info(f"➡️ Received '{parsed}' from {client_addr}")
            if not parsed:
                continue
            await CommandHandler.run_command(parsed, writer)
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
        help="The interface to listen on (default: 127.0.0.1).",
    )

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=6379,
        help="The Port to listen on (default: 6380).",
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
