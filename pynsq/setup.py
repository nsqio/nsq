from setuptools import setup

version = '0.1'

setup(name='pynsq',
      version=version,
      description="a Python reader for NSQ",
      keywords='python nsq',
      author='Matt Reiferson',
      author_email='snakes@gmail.com',
      url='http://github.com/bitly/nsq/pynsq',
      packages=['nsq'],
      include_package_data=True,
      zip_safe=True,
      )
