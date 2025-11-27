import asyncio
from pyboot.commons.coroutine.tools import getOrCreate_eventloop, run_coroutine_sync, run_coroutine_now,run_synctask  # noqa: F401
from pyboot.commons.utils.thread import Sleep
from pyboot.commons.utils.log import Logger

_logger = Logger('pyboot.test.test_asyncio')

async def async_sleep(n:int=1):
    await asyncio.sleep(n)
    _logger.DEBUG("await asyncio.sleep(1)")
    return "异步任务完成"

def normal_function():    
    result = run_coroutine_sync(async_sleep())
    _logger.DEBUG(f"结果: {result}")
    
def sync_task(n:int=6):
    Sleep(n)
    _logger.DEBUG(f"await Sleep({n})")
    return f"这是一个Sleep({n})阻塞事件"

async def print_task(n:int=6, no:str='No-1'):
    for i in range(n):
        _logger.DEBUG(f'asyncio.sleep(0.5) {no} = {i}')
        await asyncio.sleep(0.5)        




if __name__ == "__main__":
    def print_callback(c):
        # _logger.DEBUG(f'{c} {(callable(c))} {(dir(c))}')
        _logger.DEBUG(f'{c} {(callable(c))}')
    print_callback(async_sleep)
    print_callback(normal_function)
    normal_function()
    
    async def run():        
        await asyncio.sleep(10)
    
    run_coroutine_now(print_task(6, 'No-6'))
    run_synctask(sync_task)
    
    # run_coroutine_sync(run())
    
    # async def other_task():
    #  for i in range(6):
    #      await asyncio.sleep(1)
    #      print(f"其他任务执行了 {i+1} 秒")

    # async def main():
    #     a = run_coroutine_now(print_task(6))
    #     print("开始")
    #     # 不等待，只是提交任务
    #     # task = loop.run_in_executor(None, Sleep, 6)
    #     task = run_synctask(sync_task, 6)
    #     # 同时执行其他协程
    #     await a
    #     # 现在可以等待之前提交的任务（如果需要）
    #     await task
    #     print("结束")
            
    # async def main():
    #     loop,_ = getOrCreate_eventloop()
    #     print("开始")
    #     # 不等待，只是提交任务
    #     # task = loop.run_in_executor(None, Sleep, 6)
    #     task = run_synctask(sync_task, 6)
    #     # 同时执行其他协程
    #     await other_task()
    #     # 现在可以等待之前提交的任务（如果需要）
    #     await task
    #     print("结束")

    run_coroutine_now(print_task(10, 'No-10'))
    run_synctask(sync_task, 4)
    rtn = run_coroutine_sync(async_sleep(6))
    _logger.DEBUG(f'结果={rtn}')
        
    
    
    