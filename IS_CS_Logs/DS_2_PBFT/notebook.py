import asyncio
import time

async def f():
    t1 = time.time()
    await asyncio.sleep(2)
    print(f"Time taken: {time.time()-t1}")

# Run the async function using an event loop
asyncio.run(f())