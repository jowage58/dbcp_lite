# DBCP Lite

Simple, light and nearly featureless database connection pool for Python.

Connections are added to the pool as needed up to `max_size` in number.
Once added the connections will remain in the pool and no attempt is made
to shrink the pool. In most cases it is recommended to set 
`min_size == max_size`.

# Usage

```python
import cx_Oracle
from dbcp_lite import DBConnectionPool

connect_args = ('scott', 'tiger', 'dbhost.example.com/orcl')
pool = DBConnectionPool(cx_Oracle.connect, 
                        create_args=connect_args, 
                        create_kwargs={'threaded': False}, 
                        min_size=1, 
                        max_size=4, 
                        name='scott@orcl')

with pool.acquire() as connection:
    ...

with pool.acquire_cursor() as cursor:
    ...

pool.close()
```

The `acquire_cursor()` is a convenience method that returns a `Cursor` using a
`Connection` from the pool. Upon successful completion `commit()` will be
called on the `Connection` and the `Cursor` will be closed.

## Timeouts

A timeout can be provided to various methods. If no connections are available, to
be `acquired` or `closed`, the methods will raise a `queue.Empty` exception.

# Lifecycle Methods

The following lifecycle methods are called during normal operations and alternate
implementations can be provided if needed to perform specialized setup or cleanup.

`on_acquire(self, connection)` - no-op

`on_return(self, connection)` - default implementation calls `connection.rollback()`

`on_close(self, connection)` - default implementation calls `connection.close()`

For example, to implement auto-commit:

```python
pool = ...
pool.on_return = lambda x: x.commit()
``` 
