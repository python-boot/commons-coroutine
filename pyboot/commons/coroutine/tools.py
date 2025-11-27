import asyncio
import typing
import signal
import inspect
import concurrent

# 可中断 sleep -----------------------------------------------------------
async def sleep(sec: float, stop: asyncio.Event | None = None) -> None:
    """支持外部事件立即中断的 sleep"""
    if stop is None:
        await asyncio.sleep(sec)
    else:
        try:
            await asyncio.wait_for(stop.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass
        
# 限制并发量的 gather -----------------------------------------------------
async def gather_with_sem(
    coros: typing.Iterable[typing.Awaitable],
    max_concurrent: int = 10,
    *,
    return_exceptions: bool = False,
) -> typing.Any:
    """asyncio.gather 的带并发上限版本"""
    sem = asyncio.Semaphore(max_concurrent)

    async def _wrap(coro: typing.Awaitable) -> typing.Any:
        async with sem:
            return await coro

    return await asyncio.gather(
        *(_wrap(c) for c in coros), return_exceptions=return_exceptions
    )
      
        
#  超时自动取消的 gather ---------------------------------------------------
async def gather_with_timeout(
    coros: typing.Iterable[typing.Awaitable],
    timeout: float,
    *,
    return_exceptions: bool = False,
) -> typing.Any:
    return await asyncio.wait_for(
        asyncio.gather(*coros, return_exceptions=return_exceptions), timeout
    )


#  TCP 端口探活（协程版） -------------------------------------------------
async def tcp_ping(host: str, port: int, timeout: float = 3) -> bool:
    """True=端口通，False=端口不通"""
    fut = asyncio.open_connection(host, port)
    try:
        _, writer = await asyncio.wait_for(fut, timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


# 重试装饰器 -------------------------------------------------------------
def retry(
    *,
    attempts: int = 3,
    delay: float = 1,
    backoff: float = 2,
    exceptions: tuple[type[Exception], ...] = (Exception,),
):
    def decorator(func: typing.Callable) -> typing.Callable:
        async def wrapper(*args, **kwargs):
            nonlocal delay
            for _ in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    await asyncio.sleep(delay)
                    delay *= backoff
                    raise e
        return wrapper

    return decorator


# 后台定时器 --------------------------------------------------------------
def every(
    interval: float,
    *,
    stop: asyncio.Event | None = None,
):
    """异步定时器装饰器，支持外部事件停止"""

    def decorator(func: typing.Callable) -> typing.Callable:
        async def _wrapper():
            while (stop is None) or (not stop.is_set()):
                await func()
                await sleep(interval, stop)

        return _wrapper

    return decorator


# 优雅关闭辅助 -----------------------------------------------------------
def install_stop_signals(stop: asyncio.Event) -> None:
    """Ctrl-C / SIGTERM 一键设置停止事件"""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())


# 主程入口封装 -----------------------------------------------------------
def run(coro: typing.Awaitable) -> typing.Any:
    """asyncio.run 的简易别名"""
    return asyncio.run(coro)

def getOrCreate_eventloop()->asyncio.AbstractEventLoop:
    try:
        _loop = asyncio.get_event_loop()
        return _loop, False
    except RuntimeError:
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        return _loop, True

def is_coroutine_obj(coro)->bool:
    return inspect.iscoroutine(coro)

def is_coroutine_func(func:typing.Callable)->bool:
    return inspect.iscoroutinefunction(func)

def create_coroutine(func:typing.Callable, *args, **kw):
    if not is_coroutine_func(func):
        raise ValueError(f"a coroutine func was expected, got {func!r} not, 需要定义async def")
    
    """接收一个协程工厂函数，返回协程对象并调度"""
    coro = func(*args, **kw)   # 这里才真正产生协程对象    
    # return asyncio.create_task(coro)
    return coro

def run_coroutine_now(coro, loop:asyncio.AbstractEventLoop=None):
    if not is_coroutine_obj(coro):
        raise ValueError(f"a coroutine was expected, got {coro!r}")
    _loop = loop
    if _loop is None:
        _loop, _ = getOrCreate_eventloop()        
    return _loop.create_task(coro)

def run_coroutine_sync(coro, loop:asyncio.AbstractEventLoop=None)->any:
    if not is_coroutine_obj(coro):
        raise ValueError(f"a coroutine was expected, got {coro!r}")
    _loop = loop
    if _loop is None:
        _loop, _ = getOrCreate_eventloop()
        
    return _loop.run_until_complete(coro)
    
# 事件循环里直接调用同步方法(会阻塞事件循环，例如time.sleep)
def run_synctask(func:typing.Callable, *args)->any:
    if not callable(func):
        raise ValueError(f"a callable function was expected, got {func!r}")
    # 直接调用同步方法 - 会阻塞事件循环！
    # return func(*args, **kw)
    
    if is_coroutine_func(func):
        raise ValueError(f"a non coroutine callable function was expected, got {func!r}")
    
    # 更好的方式：使用线程池执行同步方法
    # 在事件循环中
    loop = asyncio.get_event_loop()
    # with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    #     result = await loop.run_in_executor(executor, func, *args)
    #     return result
    return loop.run_in_executor(None, func, *args)