from asyncio import Semaphore, Task, create_task, gather,Future,CancelledError,Queue,sleep,Lock
from typing import List, Callable, Any, TypeVar, Coroutine,Optional
import asyncio
import weakref
from pyboot.commons.utils.log import Logger
import os
import threading
from pyboot.commons.utils.utils import r_str

_logger = Logger('dataflow.utils.async.file')

WORKER_THREADS = os.cpu_count() 

class MainThreadWorkGroup:
    def __init__(self):
        self.boss_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.boss_loop)
        
    def submit(self, coro, name:str=None):
        self.boss_loop.create_task(coro, name=name)

class CoroutineWorkGroup:
    def __init__(self, worker:int=WORKER_THREADS):
        if worker < 0:
            worker = WORKER_THREADS
        # 2. 创建 worker loops（每个跑在独立线程）
        self.worker_loops:list[asyncio.AbstractEventLoop] = []
        self.idx = 0
        self.worker = worker
        i = 1
        for _ in range(WORKER_THREADS):
            w_loop = asyncio.new_event_loop()
            self.worker_loops.append(w_loop)
            # 后台线程跑 loop     
            def run(loop:asyncio.AbstractEventLoop):
                asyncio.set_event_loop(loop)
                _logger.DEBUG(f'启动第{i}个WorkGroup的EventLoop')
                loop.run_forever()
            
            t = threading.Thread(name=f'CoroutineWorkGroup-{r_str(str(i), 2, '0') if i < 10 else i}', target=run, daemon=True, kwargs={"loop":w_loop})
            # t = threading.Thread(name=f'CoroutineWorkGroup-{l_str(str(i), 2, '0') if i < 10 else i}', target=w_loop.run_forever, daemon=True)
            
            t.start()            
            i += 1
            
    def stop(self):
        _logger.DEBUG('停止Work Loop')
        for w_loop in self.worker_loops:
            # 获取所有任务并取消
            tasks = asyncio.all_tasks(loop=w_loop)
            for task in tasks:
                task.cancel()                
            # 然后运行循环直到所有任务完成（或者超时）
            # w_loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # 停止事件循环
            w_loop.call_soon_threadsafe(w_loop.stop)
        
        # 等待所有循环真正停止
        for w_loop in self.worker_loops:
            w_loop.call_soon_threadsafe(w_loop.close)
            
    def submit(self, coro):
        w_loop = self.worker_loops[self.idx % len(self.worker_loops)]
        self.idx += 1
        asyncio.run_coroutine_threadsafe(coro, w_loop)

T = TypeVar('T')

class AsyncTaskPool:
    """异步任务池 - 真正的非阻塞版本"""            
    def __init__(self, name:str='worker', max_concurrent: int=100, max_queue_size: int = 0, loop:Optional[asyncio.AbstractEventLoop]=None):
        self.semaphore = Semaphore(max_concurrent)        
        self.task_queue = Queue(maxsize=max_queue_size)
        self.worker_tasks: List[Task] = []
        self.completed = 0
        self.failed = 0
        self.is_running = False
        self.name = name or 'worker'
        self.max_concurrent = max_concurrent
        
        if loop is None:
            try:
                self.loop:asyncio.AbstractEventLoop = asyncio.get_event_loop()
            except RuntimeError:
                self.loop:asyncio.AbstractEventLoop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
        else:
            self.loop:asyncio.AbstractEventLoop = loop
            asyncio.set_event_loop(self.loop)
            
        # self._results: List[Any] = []
        # self._futures: List[Future] = []
    
    async def start(self):
        """启动任务池工作线程"""
        self.is_running = True
        # 创建工作协程来处理队列中的任务
        self.worker_tasks = [
            create_task(self._worker(f"{self.name}-{i}"))
            for i in range(self.max_concurrent)
        ]
    
    async def stop(self):
        """停止任务池"""
        self.is_running = False
        # 等待队列中所有任务完成
        await self.task_queue.join()
        # 取消工作协程
        for worker in self.worker_tasks:
            worker.cancel()
        # 等待所有工作协程结束
        await gather(*self.worker_tasks, return_exceptions=True)
        _logger.DEBUG('停止异步池完成')
    
    async def submit(self, coro_func: Callable[..., Coroutine], *args, **kwargs) -> Future:
        """非阻塞提交任务 - 立即返回 Future"""
        if not self.is_running:
            await self.start()
        
        future = Future()
        await self.task_queue.put({
            'coro_func': coro_func,
            'args': args,
            'kwargs': kwargs,
            'future': future
        })
        # self._futures.append(future)
        return future
    
    async def _worker(self, name: str):
        """工作协程 - 从队列中获取并执行任务"""
        while self.is_running:
            try:
                # 从队列获取任务
                task_data = await self.task_queue.get()
                
                async with self.semaphore:
                    try:
                        # 执行实际的协程函数
                        result = await task_data['coro_func'](*task_data['args'], **task_data['kwargs'])
                        task_data['future'].set_result(result)
                        self.completed += 1
                        # self._results.append(result)
                    except Exception as e:
                        task_data['future'].set_exception(e)
                        self.failed += 1
                        _logger.ERROR(f"任务执行失败: {e}")
                    finally:
                        # 标记任务完成
                        self.task_queue.task_done()
                        
            except CancelledError:
                break
            except Exception as e:
                _logger.ERROR(f"工作协程 {name} 错误: {e}")
    
    async def map(self, coro_func: Callable[..., Coroutine], items: List[Any]) -> List[Any]:
        """批量提交任务并等待所有结果"""
        futures = []
        for item in items:
            future = await self.submit(coro_func, item)
            futures.append(future)
        
        # 等待所有 Future 完成
        return await gather(*futures, return_exceptions=True)
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            'name':self.name,
            'max_concurrent':self.max_concurrent,
            'completed': self.completed,
            'failed': self.failed,
            'queue_size': self.task_queue.qsize(),
            'active_workers': len([t for t in self.worker_tasks if not t.done()])
        }
    
    async def wait_all(self):
        """等待所有已提交的任务完成"""
        await self.task_queue.join()
        
class AsyncEventBus:
    def __init__(self):
        self._listeners: dict[str, List[Callable]] = {}
        self._running_tasks = set()
    
    def subscribe(self, event_type: str, callback: Callable):
        """订阅事件"""
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        
        # 使用弱引用避免内存泄漏
        self._listeners[event_type].append(weakref.WeakMethod(callback) 
                                          if hasattr(callback, '__self__') 
                                          else callback)
    
    def unsubscribe(self, event_type: str, callback: Callable):
        """取消订阅"""
        if event_type in self._listeners:
            self._listeners[event_type] = [
                cb for cb in self._listeners[event_type]
                if cb != callback and 
                (not isinstance(cb, weakref.WeakMethod) or cb() != callback)
            ]
    
    async def publish(self, event_type: str, *args, **kwargs):
        """发布事件（异步）"""
        if event_type not in self._listeners:
            return
        
        for callback_ref in self._listeners[event_type]:
            try:
                if isinstance(callback_ref, weakref.WeakMethod):
                    callback = callback_ref()
                    if callback is None:
                        continue
                else:
                    callback = callback_ref
                
                if asyncio.iscoroutinefunction(callback):
                    task = asyncio.create_task(callback(*args, **kwargs))
                    self._running_tasks.add(task)
                    task.add_done_callback(self._running_tasks.discard)
                else:
                    callback(*args, **kwargs)
                    
            except Exception as e:
                print(f"Error handling event {event_type}: {e}")


class AsyncLock:
    def __init__(self):
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> 'AsyncLock':
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._lock.release()
        
class AsyncSemaphore:
    def __init__(self, value: int):
        self._semaphore = asyncio.Semaphore(value)

    async def acquire(self) -> None:
        await self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()

    async def __aenter__(self) -> 'AsyncSemaphore':
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()

class AsyncCondition:
    def __init__(self):
        self._condition = asyncio.Condition()

    async def wait(self) -> None:
        await self._condition.wait()

    async def notify(self, n: int = 1) -> None:
        self._condition.notify(n)

    async def notify_all(self) -> None:
        self._condition.notify_all()

    async def __aenter__(self) -> 'AsyncCondition':
        await self._condition.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._condition.release()


class AsyncResourceLock:
    def __init__(self, lock: Lock, resource_name: str):
        self.lock = lock
        self.resource_name = resource_name
    
    async def __aenter__(self):
        _logger.DEBUG(f"等待获取 {self.resource_name} 的锁")
        await self.lock.acquire()
        _logger.DEBUG(f"已获取 {self.resource_name} 的锁")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        _logger.DEBUG(f"释放 {self.resource_name} 的锁")
        self.lock.release()
        
        if exc_type is not None:
            _logger.DEBUG(f"资源操作异常: {exc_type.__name__}: {exc_val}")
        
        return False
                       
                
if __name__ == "__main__":
    pass

    # 使用示例
    async def mock_io_operation(item: int, delay: float = 1.0) -> str:
        """模拟 I/O 操作"""
        print(f"开始处理项目 {item}, 需要 {delay} 秒")
        await sleep(delay)
        result = f"项目 {item} 完成"
        print(result)
        return result

    async def demo_non_blocking_pool_main(loop:asyncio.AbstractEventLoop=None):
        """演示非阻塞任务池"""
        pool = AsyncTaskPool(max_concurrent=10, max_queue_size=10, loop=loop)
        
        # 立即提交多个任务，不会阻塞
        print("开始提交任务...")
        start_time = asyncio.get_event_loop().time()
        
        futures = []
        for i in range(20):
            future = await pool.submit(mock_io_operation, i, 2.0)
            futures.append(future)
            print(f"已提交任务 {i}，继续执行其他工作...")
        
        # 在任务执行期间可以做其他事情
        print("所有任务已提交，继续处理其他业务逻辑...")
        
        # 模拟其他工作
        await asyncio.sleep(1)
        print("完成了一些其他工作...")
        
        # 可以选择等待特定任务
        result_0 = await futures[0]
        print(f"任务0的结果: {result_0}")
        
        # 或者等待所有任务完成
        await pool.wait_all()
        print("所有任务完成:")
        
        # 获取统计信息
        stats = pool.get_stats()
        print(f"最终统计: {stats}")
        
        end_time = asyncio.get_event_loop().time()
        print(f"总执行时间: {end_time - start_time:.2f} 秒")
        
        await pool.stop()
        
    async def event_main():   
        # 创建一个事件总线实例
        event_bus = AsyncEventBus()
        # 示例1：订阅一个普通函数
        def normal_callback(message):
            _logger.DEBUG('执行开始')
            print(f"Normal callback received: {message}")
            _logger.DEBUG('执行完成')
            
        event_bus.subscribe("message", normal_callback)
        
        # 示例2：订阅一个异步普通函数
        async def async_normal_callback(message):
            _logger.DEBUG('执行开始')
            print(f"Async normal callback received: {message}")
            await asyncio.sleep(1.5)
            _logger.DEBUG('执行完成')
            
        event_bus.subscribe("message", async_normal_callback)                
        event_bus.subscribe("message", async_normal_callback)  
        
        await event_bus.publish("message", "Hello, World!")   
        
    # 异步锁使用示例
    async def use_async_lock():
        lock = AsyncLock()
        async with lock:
            # 临界区
            print("锁已获取，执行操作")
            await asyncio.sleep(1)     
        
    # 异步信号量使用示例
    async def use_async_semaphore():
        semaphore = AsyncSemaphore(2)  # 允许两个协程同时访问
        async with semaphore:
            print("获取信号量，执行操作")
            await asyncio.sleep(1)    
                
        
    # 异步条件变量使用示例
    async def producer(condition: AsyncCondition, queue: asyncio.Queue):
        async with condition:
            await asyncio.sleep(1)  # 模拟生产时间
            await queue.put("数据")
            _logger.DEBUG("生产者生产了数据")
            condition.notify(1)  # 通知一个消费者

    async def consumer(condition: AsyncCondition, queue: asyncio.Queue):
        async with condition:
            while queue.empty():
                await condition.wait()  # 等待条件满足
            data = await queue.get()
            _logger.DEBUG(f"消费者消费了: {data}")

    async def use_async_condition():
        condition = AsyncCondition()
        queue = asyncio.Queue()
        await asyncio.gather(
            producer(condition, queue),
            consumer(condition, queue)
        )
            
    async def demo_resource_lock_main():                        
        async def worker(lock, name, delay):
            async with AsyncResourceLock(lock, f"worker_{name}"):
                _logger.DEBUG(f"Worker {name} 开始工作")
                await asyncio.sleep(delay)
                _logger.DEBUG(f"Worker {name} 完成工作")
                
        async def demo_lock():
            lock = Lock()
            
            # 创建多个 worker 竞争同一个锁
            tasks = [
                worker(lock, "A", 2),
                worker(lock, "B", 1),
                worker(lock, "C", 1.5)
            ]            
            await asyncio.gather(*tasks)
            
        await demo_lock()
            
    # asyncio.run(demo_non_blocking_pool_main())
    # asyncio.run(event_main())
    # asyncio.run(demo_resource_lock_main())
    
    main_loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(main_loop)
    main_loop.run_until_complete(demo_non_blocking_pool_main(main_loop))
    
    # asyncio.create
        
        