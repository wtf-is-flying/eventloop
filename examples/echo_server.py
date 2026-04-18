# An asynchronous echo server.

from __future__ import annotations

import random
import socket
from datetime import timedelta

from eventloop import recv, run_until_complete, send, sleep, spawn


async def echo_server(addr: str = "127.0.0.1", port: int = 8888) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((addr, port))
    server.listen()
    server.setblocking(False)

    print(f"Echo server listening on {addr}:{port}")

    await accept_loop(server)


async def accept_loop(server: socket.socket) -> None:
    server.setblocking(False)

    while True:
        # Wait until server socket is readable (incoming connection)
        await recv(server, 0)

        try:
            client, _ = server.accept()
            client.setblocking(False)

            # Spawn a handler task for this client
            await spawn(handle_client(client))

        except BlockingIOError:
            # No connection ready (rare race)
            pass


async def handle_client(client: socket.socket) -> None:
    print("Client connected")

    try:
        while True:
            data = await recv(client, 1024)

            if not data:
                break  # client closed connection

            print(f"Received: {data!r}")
            await handle_request(client, data)

    finally:
        print("Client disconnected")
        client.close()


async def handle_request(client: socket.socket, data: bytes) -> None:
    # Sleep to simulate busy work
    await sleep(timedelta(seconds=random.random() * 10))

    await send(client, b"echo " + data)


if __name__ == "__main__":
    run_until_complete(echo_server())
