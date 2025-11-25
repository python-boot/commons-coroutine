import asyncio, time

async def job(n):
    print(f"任务{n}开始")
    await asyncio.sleep(n)
    print(f"任务{n}完成")
    return n * 10

async def ppp():
    print('123456')

async def main():
    t0 = time.time()
    # results = await asyncio.gather(job(1), job(2), job(3), ppp())
    # await job(1)
    # print(111)
    # await job(2)
    # print(222)
    # await job(3)
    # print(333)
    # await ppp()
    
    asyncio.create_task(job(1))
    asyncio.create_task(job(2))
    asyncio.create_task(job(3))
    print('开始sleep5')
    await asyncio.sleep(5)
    results = []
    print("全部结果:", results, "耗时:", time.time() - t0)

asyncio.run(main())


