"""Simple debug test to check if Progress operations are being recorded."""

import asyncio
from docket import Docket, Progress, Worker


async def main():
    docket = Docket(name="debug-docket", url="memory://", record_ttl=3600)

    async with docket:
        # Define a simple task
        async def simple_task(progress: Progress = Progress()) -> None:
            print(f"Task started, progress instance: {id(progress)}")
            print(
                f"Operations before set: {progress._operations if hasattr(progress, '_operations') else 'NO ATTR'}"
            )
            progress.set(42)
            print(f"Operations after set: {progress._operations}")
            await asyncio.sleep(0.05)

        execution = await docket.add(simple_task)()
        key = execution.key
        print(f"Task key: {key}")

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Check progress
        progress_info = await docket.get_progress(key)
        print(f"Progress info: {progress_info}")

        if progress_info:
            print(f"Current: {progress_info.current}, Total: {progress_info.total}")
        else:
            print("ERROR: Progress info is None!")


if __name__ == "__main__":
    asyncio.run(main())
