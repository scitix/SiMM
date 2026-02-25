#
# Copyright (c) 2026 Scitix Tech PTE. LTD.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ["Block", "Store"]


import asyncio
import torch
from time import perf_counter_ns

from typing import TypeAlias, List, Tuple


from . import _kv


def set_flag(name: str, value: str):
    """
    Set a global configuration flag.

    Args:
        name: Name of the flag to set.
        value: Value to assign to the flag.
    """
    return _kv.set_flag(name, value)


class Block:
    """Represents an allocated memory block with associated metadata."""

    def __init__(self, data: _kv.Data) -> None:
        """
        Initialize a Block from internal Data object.

        Args:
            data: Internal data representation of the allocated block.
        """
        self._data = data

    def as_ref(self) -> torch.Tensor:
        """
        Convert block to a PyTorch tensor reference.

        Returns:
            Tensor view of the block's data.
        """
        return self._data.AsRef()

    def view(self) -> "BlockView":
        """
        Create a view of the block for key-value operations.

        Returns:
            BlockView object for the current block.
        """
        return BlockView(_kv.DataView(self._data))


class BlockView:
    """Provides a view of a block for key-value store operations."""

    def __init__(self, data: _kv.DataView) -> None:
        """
        Initialize BlockView from internal DataView.

        Args:
            data: Internal data view representation.
        """
        self._data = data

    @classmethod
    def from_tensor(cls, tensor: torch.Tensor, mr_ext: "MrExt"):
        """
        Create a BlockView from an existing PyTorch tensor.

        Args:
            tensor: PyTorch tensor to create view from.
            mr_ext: Memory region extension for RDMA operations.

        Returns:
            BlockView representing the tensor data.
        """
        return BlockView(_kv.DataView(tensor.data_ptr(), tensor.numel() * tensor.element_size(), mr_ext))

    @classmethod
    def from_buffer(cls, buf_ptr: int, buf_size: int, mr_ext: "MrExt"):
        """
        Create a BlockView from a raw memory buffer.

        Args:
            buf_ptr: Pointer to the memory buffer.
            buf_size: Size of the buffer in bytes.
            mr_ext: Memory region extension for RDMA operations.

        Returns:
            BlockView representing the buffer.
        """
        return BlockView(_kv.DataView(buf_ptr, buf_size, mr_ext))

    def as_ref(self) -> torch.Tensor:
        """
        Convert view to a PyTorch tensor reference.

        Returns:
            Tensor view of the underlying data.
        """
        return self._data.AsRef()


# Type alias for memory region extension used in RDMA operations
MrExt: TypeAlias = _kv.MrExt


def register_mr(tensor: torch.Tensor) -> MrExt:
    """
    Register a tensor's memory for RDMA operations.

    Args:
        tensor: PyTorch tensor whose memory to register.

    Returns:
        Memory region extension handle for RDMA operations.
    """
    return _kv.register_mr(tensor.data_ptr(), tensor.numel() * tensor.element_size())


def deregister_mr(mr_ext: "MrExt") -> bool:
    """
    Deregister a previously registered memory region.

    Args:
        mr_ext: Memory region extension to deregister.

    Returns:
        True if deregistration succeeded, False otherwise.
    """
    return _kv.deregister_mr(mr_ext) == 0


class Store:
    """Key-value store interface for tensor data storage and retrieval."""

    def __init__(self) -> None:
        """Initialize an empty key-value store."""
        self._impl = _kv.KVStore()

    def allocate(self,  block_size: int) -> Block:
        """
        Allocate a new block of specified size.

        Args:
            block_size: Size of block to allocate in bytes.

        Returns:
            Newly allocated Block object.
        """
        return Block(self._impl.Allocate(block_size))

    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the store.

        Args:
            key: Key to check for existence.

        Returns:
            True if key exists, False otherwise.
        """
        return self._impl.Exists(key) == 0

    def get(self, key: str, block: BlockView) -> int:
        """
        Retrieve data associated with a key into a block view.

        Args:
            key: Key to retrieve data for.
            block: BlockView where data will be stored.

        Returns:
            On success: Size of the retrieved data in bytes (positive value).
            On error: Negative error code.
        """
        return self._impl.Get(key, block._data)

    def put(self, key: str, block: BlockView) -> int:
        """
        Store data from a block view with the given key.

        Args:
            key: Key to associate with the data.
            block: BlockView containing data to store.

        Returns:
            Status code (0 for success, non-zero for error).
        """
        return self._impl.Put(key, block._data)

    def delete(self, key: str) -> int:
        """
        Delete a key-value pair from the store.

        Args:
            key: Key to delete.

        Returns:
            Status code (0 for success, non-zero for error).
        """
        return self._impl.Delete(key)

    def mexists(self, keys: List[str]) -> List[bool]:
        """
        Batch check existence of multiple keys.

        Args:
            keys: List of keys to check.

        Returns:
            List of booleans indicating existence of each key.
        """
        rets = self._impl.MExists(keys)
        return [ret == 0 for ret in rets]

    def mput(self, keys: List[str], blocks: List[BlockView]) -> List[int]:
        """
        Batch store multiple key-value pairs.

        Args:
            keys: List of keys to store data under.
            blocks: List of BlockViews containing data to store.

        Returns:
            List of status codes for each operation.
        """
        datas: List[_kv.DataView] = []
        for block in blocks:
            datas.append(block._data)
        return self._impl.MPut(keys, datas)

    def mget(self, keys: List[str], blocks: List[BlockView]) -> List[int]:
        """
        Batch retrieve multiple key-value pairs.

        Args:
            keys: List of keys to retrieve.
            blocks: List of BlockViews to store retrieved data.

        Returns:
            List of integers where:
                - Positive value: Size of retrieved data in bytes for corresponding key.
                - Negative value: Error code for failed retrieval of corresponding key.
        """
        datas: List[_kv.DataView] = []
        for block in blocks:
            datas.append(block._data)
        return self._impl.MGet(keys, datas)

    async def exists_async(self, key: str) -> bool:
        """
        Asynchronously check if a key exists.

        Args:
            key: Key to check for existence.

        Returns:
            True if key exists, False otherwise.
        """
        ret = await _call_async_impl(self._impl.AsyncExists, key)
        return ret == 0

    async def get_async(self, key: str, block: BlockView) -> int:
        """
        Asynchronously retrieve data for a key.

        Args:
            key: Key to retrieve data for.
            block: BlockView where data will be stored.

        Returns:
            On success: Size of the retrieved data in bytes (positive value).
            On error: Negative error code.
        """
        return await _call_async_impl(self._impl.AsyncGet, key, block._data)

    async def put_async(self, key: str, block: BlockView) -> int:
        """
        Asynchronously store data with a key.

        Args:
            key: Key to associate with the data.
            block: BlockView containing data to store.

        Returns:
            Status code (0 for success, non-zero for error).
        """
        return await _call_async_impl(self._impl.AsyncPut, key, block._data)

    async def delete_async(self, key: str) -> int:
        """
        Asynchronously delete a key-value pair.

        Args:
            key: Key to delete.

        Returns:
            Status code (0 for success, non-zero for error).
        """
        return await _call_async_impl(self._impl.AsyncDelete, key)


async def _call_async_impl(fn, *args):
    start = perf_counter_ns()
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def _callback(ret):
        # If our task is killed by asyncio scheduler before callback, cpp's callback won't cause InvalidStateError
        if not future.done():
            loop.call_soon_threadsafe(future.set_result, ret)

    try:
        t1 = perf_counter_ns()
        err = await loop.run_in_executor(
            None,
            fn,
            *args,
            _callback
        )
        t2 = perf_counter_ns()
    except Exception as e:
        future.set_exception(e)
        return await future

    print(
        f"Simm-py create future: {(t1 - start)/1000:.2f} us, function time: {(t2 - t1)/1000:.2f} us")
    if err < 0:
        return err

    return await future
