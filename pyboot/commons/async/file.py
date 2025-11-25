from typing import AsyncIterator, Callable
import aiofiles
from pathlib import Path
import aiohttp
import asyncio
import os
import shutil

class AsyncFileReader:
    """异步文件读取器"""
    
    @staticmethod
    async def read_lines(filename: str | Path, encoding='utf-8', process_fn:Callable=None) -> AsyncIterator[str]:
        """异步逐行读取文件"""
        file_path = Path(filename)
        total_size = file_path.stat().st_size
        # bytes_read = 0
        async with aiofiles.open(file_path, mode='r', encoding=encoding) as f:
            async for line in f:
                bytes_read = line.encode(encoding)                        
                if callable(process_fn):
                    rtn = process_fn(total_size, line, bytes_read+1)
                    if rtn:
                        break 
                yield line.strip()
                
    @staticmethod
    async def read_bytes(filename: str | Path, chunk_size: int = 64 * 1024, process_fn:Callable=None) -> AsyncIterator[bytes]:     
        file_path = Path(filename)   
        total_size = file_path.stat().st_size
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:  # EOF
                    break                
                if callable(process_fn):
                    rtn = process_fn(total_size, chunk, len(chunk))
                    if rtn:
                        break                
                yield chunk
    
    @staticmethod    
    # 5. 异步文件下载（带进度条） -----------------------------------------------
    async def download(
        url: str,
        fpath: str | Path = ".",
        *,
        chunk_size: int = 1024 * 64,
        session: aiohttp.ClientSession | None = None,
        process_fn:Callable
    ) -> Path:        
        folder = Path(fpath)
        folder.parent.mkdir(parents=True, exist_ok=True)
        # fname = url.split("/")[-1] or "file"
        # fpath = folder / fname
        fpath = folder

        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                # pbar = tqdm.tqdm(total=total, unit="B", unit_scale=True, desc=fname)
                async with aiofiles.open(fpath, "wb") as fp:
                    async for chunk in resp.content.iter_chunked(chunk_size):                        
                        await fp.write(chunk)
                        if callable(process_fn):
                            rtn = process_fn(total, chunk, len(chunk))
                            if rtn:
                                break
                        # pbar.update(len(chunk))
                # pbar.close()
        finally:
            if close_session:
                await session.close()
        return total
    
    @staticmethod
    async def write_text(
        path: str | Path, data: str, encoding: str = "utf-8"
    ) -> None:        
        folder = Path(path)
        folder.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiofiles.open(path, "w", encoding=encoding) as f:
            await f.write(data)
      
    @staticmethod      
    async def write_bytes(path: str|Path, data: bytes, *, chunk_size: int = 64 * 1024,  # 64 KB
                          process_fn:Callable) -> None:
        """
        异步写二进制文件，支持按块写入 + 实时进度条
        用法：
            await write_bytes("big.iso", b"...", chunk_size=128*1024)
        """
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)  # 自动创建父目录
        total = len(data)            
        # 按块写
        async with aiofiles.open(file_path, "wb") as f:
            for offset in range(0, total, chunk_size):
                chunk = data[offset : offset + chunk_size]
                await f.write(chunk)
                if callable(process_fn):
                    rtn = process_fn(total, chunk, len(chunk))
                    if rtn:
                        break
    
    @staticmethod                
    # ---------- 异步复制文件 ----------
    async def copy(
                src: str|Path,
                dst: str|Path,
                *,
                chunk_size: int = 1024 * 1024,
                process_fn:Callable
            ) -> None:
        """大文件异步复制，分块 + 进度条"""
        src_path, dst_path = Path(src), Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        total = src_path.stat().st_size
        
        async with aiofiles.open(src_path, "rb") as f_src:
            async with aiofiles.open(dst_path, "wb") as f_dst:
                while chunk := await f_src.read(chunk_size):
                    await f_dst.write(chunk)
                    if callable(process_fn):
                        rtn = process_fn(total, chunk, len(chunk))
                        if rtn:
                            break
                            
    @staticmethod    
    # ---------- 异步移动文件 ----------
    async def move(src: str|Path, dst: str|Path) -> None:
        """异步移动（同盘即 rename，跨盘先 copy 后 delete）"""
        src_path, dst_path = Path(src), Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if src_path.parent.resolve() == dst_path.parent.resolve():
            # 同盘直接 rename
            await asyncio.get_event_loop().run_in_executor(None, src_path.rename, dst_path)
        else:
            # 跨盘：copy + delete
            await AsyncFileReader.copy(src, dst)
            await AsyncFileReader.remove(src)

    @staticmethod
    # ---------- 7. 异步删除文件 ----------
    async def remove(path: str|Path) -> None:
        """异步删除文件"""
        await asyncio.get_event_loop().run_in_executor(None, os.remove, Path(path))

    @staticmethod    
    # ---------- 8. 异步删除目录 ----------
    async def rmtree(path: str|Path) -> None:
        """异步递归删除目录"""
        await asyncio.get_event_loop().run_in_executor(None, shutil.rmtree, Path(path))


                    
                        
