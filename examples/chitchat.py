from __future__ import annotations

from datetime import timedelta

from eventloop import (
    clock,
    join,
    nothing,
    recv,
    run_until_complete,
    send,
    sleep,
    socketpair,
    spawn,
)


async def hello(name: str) -> None:
    await nothing()
    print(f"Hello, {name}!")


async def hello_delayed(name: str, delay: timedelta) -> None:
    await sleep(delay)
    print(f"H-H-H-Hello, {name}!")


async def main() -> None:
    left, right = socketpair()

    # Recover the child task handle.
    alice = await spawn(hello("Alice"))

    # Progress the event loop, for debugging
    for _ in range(10):
        await nothing()

    # Send data on a socket in the background
    await spawn(send(left, b"hello Shadowheart!! how are you doing???"))
    received = await recv(right, 10)
    print(f"Received (right): {received}")

    # Send data the other way around
    await spawn(send(right, b"hello nice to meet you :))"))
    received = await recv(left, 10)
    print(f"Received (left): {received}")

    # Test sleep
    start = clock()
    _ = await spawn(hello_delayed("Bob", timedelta(seconds=2)))
    delay = timedelta(seconds=5)
    print(f"Sleeping for {delay.total_seconds()} s")
    await sleep(delay)
    duration = clock() - start
    print(f"Slept for {duration} s")

    # Wait for the child task to complete.
    await join(alice)
    print("After alice")

    await hello("Carl")
    print("After bob")


if __name__ == "__main__":
    run_until_complete(main())
