import logging
import contextlib
import queue
import threading
from typing import Callable, Dict, Iterator, Tuple


logger = logging.getLogger('dbcp_lite')


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
        self.name = name
        self._create_args = create_args if create_args else ()
        self._create_kwargs = create_kwargs if create_kwargs else {}
        self.on_create = create_func
        self.min_size = min_size
        self.max_size = max_size
        self._pool = queue.SimpleQueue()
        for i in range(min_size):
            conn = create_func(*self._create_args, **self._create_kwargs)
            self._pool.put_nowait(conn)
        self._size = min_size
        self._closed = False
        self._lock = threading.Lock()

    def on_create(self, *args, **kwargs):
        raise NotImplementedError

    def on_acquire(self, connection):
        return connection

    def on_release(self, connection):
        return connection

    def on_close(self, connection):
        return connection

    @contextlib.contextmanager
    def acquire(self, timeout: float = 60.0) -> Iterator:
        if self._closed:
            raise RuntimeError('Pool is closed')
        if self._size < self.max_size:
            conn = self._try_get_or_create()
            if conn is None:
                return self.acquire(timeout)
        else:
            conn = self._pool.get(block=True, timeout=timeout)
        conn = self.on_acquire(conn)
        try:
            yield conn
        finally:
            conn = self.on_release(conn)
            self.release(conn)

    @contextlib.contextmanager
    def acquire_cursor(self, timeout: float = 60.0) -> Iterator:
        with self.acquire(timeout) as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            finally:
                cursor.close()

    def release(self, connection) -> None:
        try:
            connection.rollback()
        except:
            logger.warning('releasing connection failed: %s', connection)
            connection = self.on_create(*self._create_args, **self._create_kwargs)
        if self._closed:
            connection = self.on_close(connection)
            self._close_conn(connection)
        else:
            self._pool.put_nowait(connection)

    def close(self) -> None:
        logger.info('Closing pool: %s', self)
        self._closed = True
        while True:
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                break
            else:
                try:
                    conn = self.on_close(conn)
                    self._close_conn(conn)
                except:
                    logger.warning('Failed to close connection')

    def _try_get_or_create(self):
        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._size < self.max_size:
                    self._size += 1
                    conn = self.on_create(*self._create_args, **self._create_kwargs)
                    logger.debug('added connection to pool: %s', conn)
                else:
                    conn = None
        return conn

    def _close_conn(self, connection) -> None:
        try:
            connection.close()
        except:
            logger.exception('Failed to close connection: %s', connection)

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
