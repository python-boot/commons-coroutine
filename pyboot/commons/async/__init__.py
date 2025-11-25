import asyncio, aiohttp, aiofiles, os, time, typing, tqdm, signal


# 1. 可中断 sleep -----------------------------------------------------------
async def sleep(sec: float, stop: asyncio.Event | None = None) -> None:
    """支持外部事件立即中断的 sleep"""
    if stop is None:
        await asyncio.sleep(sec)
    else:
        try:
            await asyncio.wait_for(stop.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass
        
# 2. 限制并发量的 gather -----------------------------------------------------
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
        
# 3. 超时自动取消的 gather ---------------------------------------------------
async def gather_with_timeout(
    coros: typing.Iterable[typing.Awaitable],
    timeout: float,
    *,
    return_exceptions: bool = False,
) -> typing.Any:
    return await asyncio.wait_for(
        asyncio.gather(*coros, return_exceptions=return_exceptions), timeout
    )


# 4. TCP 端口探活（协程版） -------------------------------------------------
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

        



# 6. 重试装饰器 -------------------------------------------------------------
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


# 7. 后台定时器 --------------------------------------------------------------
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


# 8. 优雅关闭辅助 -----------------------------------------------------------
def install_stop_signals(stop: asyncio.Event) -> None:
    """Ctrl-C / SIGTERM 一键设置停止事件"""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())


# 9. 主程入口封装 -----------------------------------------------------------
def run(coro: typing.Awaitable) -> typing.Any:
    """asyncio.run 的简易别名"""
    return asyncio.run(coro)