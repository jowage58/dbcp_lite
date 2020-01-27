import contextlib
import functools
import logging
import queue
import threading
from typing import Callable, Dict, Tuple

try:
    from queue import SimpleQueue as PoolQueue
except ImportError:
    from queue import Queue as PoolQueue

logger = logging.getLogger(__name__)


class PoolTimeout(Exception):
    pass


class DBConnectionPool:
    """A simple queue backed database connection pool."""

    def __init__(self, create_func: Callable,
                 create_args: Tuple = None,
                 create_kwargs: Dict = None,
                 min_size: int = 1,
                 max_size: int = 4,
                 name: str = None) -> None:
        assert create_func, 'A create function must be provided'
        assert 1 <= min_size <= max_size <= 32, (
            f'Pool size out of range: min={min_size}, max={max_size}'
        )
        if create_args is None:
            create_args = ()
        if create_kwargs is None:
            create_kwargs = {}
        create_func = functools.partial(create_func, *create_args, **create_kwargs)
        pool = PoolQueue()
        for conn in [create_func() for _ in range(min_size)]:
            pool.put_nowait(conn)
        self._create_func = create_func
        self._pool = pool
        self._size = min_size
        self._closed = False
        self._lock = threading.Lock()
        self.min_size = min_size
        self.max_size = max_size
        self.name = name

    def on_acquire(self, connection) -> None:
        """Called when the connection is acquired.

        The default implementation is a no-op.
        """
        pass

    def on_return(self, connection) -> None:
        """Called when the connection is being returned to the pool.

        The default implementation calls `connection.rollback()`. In order
        to enable auto-commit you can replace this function with one that
        calls `connection.commit()`.
        """
        connection.rollback()

    def on_close(self, connection) -> None:
        """Called when the connection is being removed from the pool.

        Connections are removed from the pool only after the pool `close()`
        method is called. The default implementation calls `connection.close()`.
        """
        connection.close()

    @contextlib.contextmanager
    def acquire(self, timeout: float = 60.0):
        """Provides a Connection from the pool.

        For use in a with block and upon exit the provided connection
        will be returned to the pool. Callers are expected to NOT call the close
        method on the connection. If a Connection is not available before `timeout`
        seconds a `queue.Empty` exception will be raised.
        """
        if self._closed:
            raise RuntimeError('Pool is closed')
        if self._size < self.max_size:
            conn = self._try_get_or_create()
            if conn is None:
                return self.acquire(timeout)
        else:
            conn = self._pool_get(timeout)
        try:
            self.on_acquire(conn)
            yield conn
        finally:
            try:
                self.on_return(conn)
            finally:
                self._pool.put_nowait(conn)

    @contextlib.contextmanager
    def acquire_cursor(self, timeout: float = 60.0):
        """Provides a Cursor using a Connection from the pool.

        This is a convenience method that returns a Cursor and upon successful
        completion of the with block will call `connection.commit()` and close
        the Cursor. If a Connection is not available before `timeout`
        seconds a `queue.Empty` exception will be raised.
        """
        with self.acquire(timeout) as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            finally:
                cursor.close()

    def close(self, timeout: float = 120) -> None:
        """Close the pool and the Connections it contains.

        After a call to close the pool should no longer be used. Calls
        to `acquire()` will fail with a `RuntimeError`. If any of the pool's
        Connections are not available before `timeout` seconds a `queue.Empty`
        exception will be raised.
        """
        logger.debug('Closing pool: %s', self)
        self._closed = True
        with self._lock:
            pool_size = self._size
        while pool_size:
            conn = self._pool_get(timeout)
            pool_size -= 1
            try:
                self.on_close(conn)
            except Exception as e:
                logger.warning('Failed to close connection: %s - %r', conn, e)

    def _try_get_or_create(self):
        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if not self._closed and self._size < self.max_size:
                    conn = self._create_func()
                    self._size += 1
                    logger.debug('Adding connection to pool: %s', conn)
                else:
                    conn = None
        return conn

    def _pool_get(self, timeout):
        try:
            return self._pool.get(block=True, timeout=timeout)
        except queue.Empty:
            raise PoolTimeout('No connections available before timeout: %s', timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        return (
            f"DBConnectionPool(name='{self.name}', min_size={self.min_size}, "
            f"max_size={self.max_size}, current_size={self._size}, "
            f"closed={self._closed})"
        )


__all__ = ['DBConnectionPool', 'PoolTimeout']
