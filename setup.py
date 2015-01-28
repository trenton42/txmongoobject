from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='txmongoobject',
      version='0.2.4',
      description='Object models in Mongo',
      long_description=readme(),
      keywords='mongo twisted',
      url='http://github.com/trenton42/txmongoobject',
      author='Trenton Broughton',
      author_email='trenton@devpie.com',
      license='MIT',
      packages=[
          'txmongoobject',
      ],
      install_requires=[
          'txmongo',
          'pymongo',
          'pytz'
      ],
      zip_safe=False)
