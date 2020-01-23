# dbcp_lite

Simple, light and nearly featureless database connection pool for Python.

# Usage

```python
import cx_Oracle
from dbcp_lite import DBConnectionPool

connect_args = ('scott', 'tiger', 'dbhost.example.com/orcl')
pool = DBConnectionPool(cx_Oracle.connect, create_args=connect_args)

with pool.acquire() as connection:
    ...

with pool.acquire_cursor() as cursor:
    ...
```
