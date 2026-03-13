"""
Unit tests for the key-value store functionality.

This module contains comprehensive tests for the SiMM key-value store,
covering both synchronous and asynchronous operations with and without
memory registration (MR) for RDMA operations.
"""

import asyncio
import pytest
import random
import torch
import ctypes


from simm import kv
from simm.kv import Store, BlockView, register_mr


# Global registry to keep references to allocated memory buffers
# This prevents garbage collection while buffers are in use
main_registry = {}

# Test data block sizes ranging from small to large
BLOCK_SIZE = [
    500,                    # 500B
    1024,                   # 1KB
    1950,                   # 1.9KB
    2250,                   # 2.2KB
    4096,                   # 4KB
    8192,                   # 8KB
    8900,                   # 8.7KB
    130 * 1024,             # 130KB
    1 << 18,                # 256KB
    440 * 1024,             # 440KB
    1 << 20,                # 1MB
    (1 << 20) + 102400,     # 1.1MB
    (1 << 20) + (1 << 19),  # 1.5MB
    3 * (1 << 20),          # 3MB
    1 << 22,                # 4MB
]

# Constants for memory calculations
ONE_MB = 1 << 20
BUFFER_SIZE = 1 << 36  # 64GB - Large buffer for memory registration tests


def key_as_tensor(key: str):
    """
    Convert a string key to a PyTorch tensor.

    Args:
        key: String key to convert.

    Returns:
        Tensor containing the UTF-8 bytes of the key.
    """
    return torch.frombuffer(bytearray(key.encode()), dtype=torch.uint8)


@pytest.fixture
def setup():
    """
    Fixture for setting up basic test environment without memory registration.

    Sets up configuration, creates a store instance, and allocates test blocks
    of various sizes for testing standard key-value operations.

    Returns:
        Tuple containing:
            make_key: Function to generate unique test keys
            store_: Store instance for testing
            datas_: List of allocated Block objects of various sizes
    """
    # Configure connection manager IP (update to your own IP)
    kv.set_flag("cm_primary_node_ip", "0.0.0.0")
    key = random.randint(1, 1000)

    def make_key(pass_: str, index: int):
        """
        Generate a unique test key.

        Args:
            pass_: Test phase identifier.
            index: Unique index for the key.

        Returns:
            Unique test key string.
        """
        return f"py-T{pass_}-{key}-{index}"

    store_ = kv.Store()
    datas_ = [store_.allocate(block_size) for block_size in BLOCK_SIZE]

    return make_key, store_, datas_


@pytest.fixture
def setup_mr():
    """
    Fixture for setting up test environment with memory registration.

    Creates a large pinned memory buffer, registers it for RDMA operations,
    and provides a function to allocate views into this buffer.

    Returns:
        Tuple containing:
            make_key: Function to generate unique test keys
            allocate_buffer: Function to allocate BlockViews from registered buffer
            store_: Store instance for testing
    """
    global main_registry

    # Create a large pinned memory buffer and register it for RDMA
    buffer = torch.empty(BUFFER_SIZE, dtype=torch.uint8, pin_memory=True)
    buffer.fill_(0)
    ptr = buffer.data_ptr()
    main_registry[ptr] = buffer  # Keep reference to prevent garbage collection

    mr_ext = register_mr(buffer)

    # Configure connection manager IP (update to your own IP)
    kv.set_flag("cm_primary_node_ip", "0.0.0.0")
    key = random.randint(1, 1000)
    store_ = kv.Store()

    def make_key(pass_: str, index: int):
        """
        Generate a unique test key.

        Args:
            pass_: Test phase identifier.
            index: Unique index for the key.

        Returns:
            Unique test key string.
        """
        return f"py-T{pass_}-{key}-{index}"

    buffer_offset: int = 0

    def allocate_buffer(block_size: int) -> BlockView:
        """
        Allocate a BlockView from the registered memory buffer.

        Args:
            block_size: Size of block to allocate in bytes.

        Returns:
            BlockView pointing to a slice of the registered buffer.
        """
        nonlocal buffer_offset
        raw_data = buffer[buffer_offset: buffer_offset + block_size]
        buffer_offset += block_size
        return BlockView.from_tensor(raw_data, mr_ext)

    return make_key, allocate_buffer, store_


def test_kv_put_get_one(setup):
    """
    Test basic put and get operations for single key-value pairs.

    For each block size, tests:
    1. Storing data with a unique key
    2. Retrieving the data and verifying it matches what was stored

    Args:
        setup: Fixture providing test environment.
    """
    make_key, store_, datas_ = setup

    for i in range(len(BLOCK_SIZE)):
        key = make_key("PutOneGetOne", BLOCK_SIZE[i])
        key_ = key_as_tensor(key)
        data_ = datas_[i]
        data = data_.as_ref()
        data[: len(key_)] = key_
        assert store_.put(key, data_.view()) == 0

        # Verify key exists and data retrieval
        assert store_.exists(key)

        got = store_.allocate(BLOCK_SIZE[i])
        assert store_.get(key, got.view()) > 0
        got_ = got.as_ref()
        assert all(got_[: len(key_)] == key_)


def test_kv_put_get_one_mr(setup_mr):
    """
    Test basic put and get operations with memory registration.

    Similar to test_kv_put_get_one but uses memory-registered buffers
    for RDMA operations.

    Args:
        setup_mr: Fixture providing test environment with memory registration.
    """
    make_key, allocate_buffer, store_ = setup_mr

    for i in range(len(BLOCK_SIZE)):
        block_size = BLOCK_SIZE[i]
        key = make_key("PutOneGetOne", block_size)
        key_ = key_as_tensor(key)
        data_ = allocate_buffer(block_size)
        data = data_.as_ref()
        data[: len(key_)] = key_
        assert store_.put(key, data_) == 0

        # Verify key exists and data retrieval
        assert store_.exists(key)

        got = allocate_buffer(block_size)
        assert store_.get(key, got) > 0
        got_ = got.as_ref()
        assert all(got_[: len(key_)] == key_)


def test_kv_put_many_get_many(setup):
    """
    Test storing and retrieving many key-value pairs.

    Tests bulk operations by storing many values with the same block size,
    then retrieving and verifying each one.

    Args:
        setup: Fixture providing test environment.
    """
    make_key, store_, datas_ = setup

    PUT_MANY_GET_MANY = 1000

    for j in range(len(BLOCK_SIZE)):
        data_ = datas_[j]
        data = data_.as_ref()
        for i in range(PUT_MANY_GET_MANY):
            key = make_key("PutManyGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            data[: len(key_)] = key_
            assert store_.put(key, data_.view()) == 0

        for i in range(PUT_MANY_GET_MANY):
            key = make_key("PutManyGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)

            assert store_.exists(key)

            got = store_.allocate(BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            assert store_.get(key, got.view()) > 0
            got_ = got.as_ref()
            print(
                f'key: {key} value: {str(got_[: len(key_)].numpy().tobytes())}')
            assert all(got_[: len(key_)] == key_)


def test_kv_put_many_get_many_mr(setup_mr):
    """
    Test storing and retrieving many key-value pairs with memory registration.

    Similar to test_kv_put_many_get_many but uses memory-registered buffers.

    Args:
        setup_mr: Fixture providing test environment with memory registration.
    """
    make_key, allocate_buffer, store_ = setup_mr

    PUT_MANY_GET_MANY = 200
    total_needed = sum(bs * 2 * PUT_MANY_GET_MANY for bs in BLOCK_SIZE)
    print(f"Test needs {total_needed / 1024**2:.2f} MB memory")

    for j in range(len(BLOCK_SIZE)):
        block_size = BLOCK_SIZE[j]
        datas = []
        for i in range(PUT_MANY_GET_MANY):
            data_ = allocate_buffer(block_size)
            data: torch.Tensor = data_.as_ref()
            key = make_key("PutManyGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            data[: len(key_)] = key_
            datas.append(data_)
            assert store_.put(key, data_) == 0

        gots = []
        for i in range(PUT_MANY_GET_MANY):
            key = make_key("PutManyGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)

            assert store_.exists(key)

            got_ = allocate_buffer(block_size)
            assert store_.get(key, got_) > 0
            got = got_.as_ref()
            print(
                f'key: {key} value: {str(got[: len(key_)].numpy().tobytes())}')
            assert all(got[: len(key_)] == key_)


def test_kv_mput_many_mget_many(setup):
    """
    Test batch put and get operations for multiple key-value pairs.

    Uses mput and mget methods to test batch operations for efficiency.

    Args:
        setup: Fixture providing test environment.
    """
    make_key, store_, datas_ = setup

    PUT_MANY_GET_MANY = 200

    for j in range(len(BLOCK_SIZE)):
        block_size = BLOCK_SIZE[j]
        keys = []
        datas = []
        for i in range(PUT_MANY_GET_MANY):
            data_ = store_.allocate(block_size)
            data = data_.as_ref()
            key = make_key("MPutManyMGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            data[: len(key_)] = key_
            keys.append(key)
            datas.append(data_.view())
        rets = store_.mput(keys, datas)
        for ret in rets:
            assert ret >= 0

        # check exists
        rets = store_.mexists(keys)
        assert all(rets)

        gots = [store_.allocate(block_size) for i in range(PUT_MANY_GET_MANY)]
        gots_ = [data.view() for data in gots]
        rets = store_.mget(keys, gots_)
        for ret in rets:
            assert ret >= 0
        for i in range(PUT_MANY_GET_MANY):
            key = make_key("MPutManyMGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            got_ = gots[i].as_ref()
            print(
                f'key: {key} value: {str(got_[: len(key_)].numpy().tobytes())}')
            assert all(got_[: len(key_)] == key_)


def test_kv_mput_many_mget_many_mr(setup_mr):
    """
    Test batch put and get operations with memory registration.

    Similar to test_kv_mput_many_mget_many but uses memory-registered buffers.

    Args:
        setup_mr: Fixture providing test environment with memory registration.
    """
    make_key, allocate_buffer, store_ = setup_mr
    PUT_MANY_GET_MANY = 200

    total_needed = sum(bs * 2 * PUT_MANY_GET_MANY for bs in BLOCK_SIZE)
    print(f"Test needs {total_needed / 1024**2:.2f} MB memory")

    for j in range(len(BLOCK_SIZE)):
        block_size = BLOCK_SIZE[j]
        keys = []
        datas = []
        for i in range(PUT_MANY_GET_MANY):
            # data_ = store_.allocate(block_size)
            data_ = allocate_buffer(block_size)
            data: torch.Tensor = data_.as_ref()
            key = make_key("MPutManyMGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            data[: len(key_)] = key_
            keys.append(key)
            datas.append(data_)
        rets = store_.mput(keys, datas)
        for ret in rets:
            assert ret >= 0

        # check exists
        rets = store_.mexists(keys)
        assert all(rets)

        # gots = [store_.allocate(block_size) for i in range(PUT_MANY_GET_MANY)]
        gots = []
        for i in range(PUT_MANY_GET_MANY):
            got_ = allocate_buffer(block_size)
            gots.append(got_)
        rets = store_.mget(keys, gots)
        for ret in rets:
            assert ret >= 0
        for i in range(PUT_MANY_GET_MANY):
            key = make_key("MPutManyMGetMany", i +
                           BLOCK_SIZE[j] * PUT_MANY_GET_MANY)
            key_ = key_as_tensor(key)
            got_ = gots[i].as_ref()
            print(
                f'key: {key} value: {str(got_[: len(key_)].numpy().tobytes())}')
            assert all(got_[: len(key_)] == key_)


@pytest.mark.asyncio
async def test_kv_put_get_one_async(setup):
    """
    Test asynchronous put and get operations for single key-value pairs.

    Tests async API for basic store/retrieve operations.

    Args:
        setup: Fixture providing test environment.
    """
    make_key, store_, datas_ = setup

    for i in range(len(BLOCK_SIZE)):
        size = BLOCK_SIZE[i]
        key = make_key("PutOneGetOneA", BLOCK_SIZE[i])
        key_ = key_as_tensor(key)
        data_ = datas_[i]
        data = data_.as_ref()
        data[: len(key_)] = key_
        ret = await store_.put_async(key, data_.view())
        assert (ret == 0)

        assert await store_.exists_async(key)

        got = store_.allocate(size)
        ret = await store_.get_async(key, got.view())
        assert (ret > 0)
        got_ = got.as_ref()
        assert (got_[: len(key_)] == key_)


@pytest.mark.asyncio
async def test_kv_put_get_one_async_mr(setup_mr):
    """
    Test asynchronous put and get operations with memory registration.

    Similar to test_kv_put_get_one_async but uses memory-registered buffers.

    Args:
        setup_mr: Fixture providing test environment with memory registration.
    """
    make_key, allocate_buffer, store_ = setup_mr

    for size in BLOCK_SIZE:
        key = make_key("PutOneGetOneA", size)
        key_ = key_as_tensor(key)
        data_ = allocate_buffer(size)
        data = data_.as_ref()
        data[: len(key_)] = key_
        ret = await store_.put_async(key, data_.view())
        assert (ret == 0)

        assert await store_.exists_async(key)

        got = allocate_buffer(size)
        ret = await store_.get_async(key, got.view())
        assert (ret > 0)
        got_ = got.as_ref()
        assert (got_[: len(key_)] == key_)


@pytest.mark.asyncio
async def test_kv_put_many_get_many_async(setup):
    """
    Test asynchronous bulk put and get operations.

    Tests async API for storing and retrieving many key-value pairs.

    Args:
        setup: Fixture providing test environment.
    """
    make_key, store_, _ = setup

    PUT_MANY_GET_MANY_ASYNC = 1000

    datas, futures = [], []
    for size in BLOCK_SIZE:
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            data = store_.allocate(size)
            data_ = data.as_ref()
            data_[: len(key_)] = key_

            datas.append(data)
            futures.append(store_.put_async(key, data.view()))
        oks = await asyncio.gather(*futures)
        assert all(oks)

        futures = []
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i)
            futures.append(store_.exists_async(key))
        oks = await asyncio.gather(*futures)
        assert all(oks)

        gots, futures = [], []
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            got = store_.allocate(size)
            gots.append(got)
            futures.append(store_.get_async(key, got.view()))
        oks = await asyncio.gather(*futures)
        assert all(oks)

        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            got = gots[i]
            got_ = got.as_ref()
            assert all(got_[: len(key_)] == key_)


@pytest.mark.asyncio
async def test_kv_put_many_get_many_async_mr(setup_mr):
    """
    Test asynchronous bulk put and get operations with memory registration.

    Similar to test_kv_put_many_get_many_async but uses memory-registered buffers.

    Args:
        setup_mr: Fixture providing test environment with memory registration.
    """
    make_key, allocate_buffer, store_ = setup_mr

    PUT_MANY_GET_MANY_ASYNC = 200

    datas, futures = [], []
    for size in BLOCK_SIZE:
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            data = allocate_buffer(size)
            data_ = data.as_ref()
            data_[: len(key_)] = key_

            datas.append(data)
            futures.append(store_.put_async(key, data.view()))
        rets = await asyncio.gather(*futures)
        assert all([ret == 0 for ret in rets])

        futures = []
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i)
            futures.append(store_.exists_async(key))
        oks = await asyncio.gather(*futures)
        assert all(oks)

        gots, futures = [], []
        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            got = allocate_buffer(size)
            gots.append(got)
            futures.append(store_.get_async(key, got.view()))
        oks = await asyncio.gather(*futures)
        assert all(oks)

        for i in range(PUT_MANY_GET_MANY_ASYNC):
            key = make_key("PutManyGetManyA", i +
                           size * PUT_MANY_GET_MANY_ASYNC)
            key_ = key_as_tensor(key)

            got = gots[i]
            got_ = got.as_ref()
            assert got_[: len(key_)] == key_
