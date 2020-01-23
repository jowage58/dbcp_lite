import queue
import unittest
from unittest.mock import Mock, call

from dbcp_lite import DBConnectionPool


class TestPoolInit(unittest.TestCase):

    def test_must_provide_create_func(self):
        with self.assertRaises(AssertionError):
            DBConnectionPool(create_func=None)

    def test_min_size(self):
        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), min_size=0)

        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), min_size=-1)

        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), min_size=2, max_size=1)

    def test_max_size(self):
        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), max_size=0)

        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), max_size=-1)

        with self.assertRaises(AssertionError):
            DBConnectionPool(Mock(), max_size=33)

    def test_creates_min_number_of_connections(self):
        pool = DBConnectionPool(Mock(), min_size=2)
        self.assertEqual(len(pool), 2)

    def test_calls_create_func_with_args(self):
        create_func = Mock()
        args = None
        DBConnectionPool(create_func, create_args=args)
        create_func.assert_called_with()

        args = (1, 2)
        DBConnectionPool(create_func, create_args=args)
        create_func.assert_called_with(1, 2)

        kwargs = {'one': 1, 'two': 2}
        DBConnectionPool(create_func, create_kwargs=kwargs)
        create_func.assert_called_with(one=1, two=2)

        args = (1, 2)
        kwargs = {'one': 1, 'two': 2}
        DBConnectionPool(create_func, create_args=args, create_kwargs=kwargs)
        create_func.assert_called_with(1, 2, one=1, two=2)

    def test_fails_to_create_connection(self):
        create_func = Mock(side_effect=RuntimeError)
        self.assertRaises(RuntimeError, DBConnectionPool, create_func)


class TestPoolAcquire(unittest.TestCase):

    def test_acquire_fails_if_pool_closed(self):
        pool = DBConnectionPool(Mock())
        pool.close()
        with self.assertRaises(RuntimeError):
            with pool.acquire() as p:
                pass

    def test_acquire_calls_on_acquire(self):
        mock_conn = Mock()
        create_func = Mock(return_value=mock_conn)
        pool = DBConnectionPool(create_func, min_size=1, max_size=1)
        pool.on_acquire = Mock()
        with pool.acquire() as conn:
            self.assertEqual(conn, mock_conn)
        pool.on_acquire.assert_called_with(mock_conn)

    def test_acquire_fails_if_on_acquire_fails(self):
        mock_conn = Mock()
        create_func = Mock(return_value=mock_conn)
        pool = DBConnectionPool(create_func, min_size=1, max_size=1)
        pool.on_acquire = Mock(side_effect=RuntimeError)
        pool.on_return = Mock()
        with self.assertRaises(RuntimeError):
            with pool.acquire() as conn:
                pass
        pool.on_return.assert_called_with(mock_conn)

    def test_connection_returned_if_on_release_fails(self):
        mock_conn = Mock()
        create_func = Mock(return_value=mock_conn)
        pool = DBConnectionPool(create_func, min_size=1, max_size=1)
        pool.on_release = Mock(side_effect=RuntimeError)
        with pool.acquire() as _:
            pass
        self.assertEqual(pool._pool.get_nowait(), mock_conn)

    def test_no_connection_available_by_timeout(self):
        mock_conn = Mock()
        create_func = Mock(return_value=mock_conn)
        pool = DBConnectionPool(create_func, min_size=1, max_size=1)
        with pool.acquire() as _:
            with self.assertRaises(queue.Empty):
                with pool.acquire(timeout=0.1) as _:
                    pass


class TestPoolClose(unittest.TestCase):

    def test_conn_close_called_on_close(self):
        mock_conn = Mock()
        create_func = Mock(return_value=mock_conn)
        pool = DBConnectionPool(create_func, min_size=2, max_size=2)
        pool.close()
        mock_conn.assert_has_calls([call.close(), call.close()])
