import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dbcp-lite',
    version='0.0.1',
    description='Simple, light and nearly featureless database connection pool',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jowage58/dbcp_lite/',
    author='John Wagenleitner',
    author_email='johnwa@mail.fresnostate.edu',
    keywords='database connection pool',
    py_modules=['dbcp_lite'],
    python_requires='>=3.6, <4',
    project_urls={
        'Bug Reports': 'https://github.com/jowage58/dbcp_lite/issues',
        'Source': 'https://github.com/jowage58/dbcp_lite/',
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
