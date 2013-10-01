from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='txmongoobject',
      version='0.2.1',
      description='Object models in Mongo',
      long_description=readme(),
      keywords='mongo twisted',
      url='http://github.com/trenton42/txmongoobject',
      author='Trenton Broughton',
      author_email='trenton@devpie.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'twisted',
          'txmongo'
      ],
      zip_safe=False)
