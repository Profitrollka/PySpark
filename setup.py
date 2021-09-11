from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='pysp_lib',
      version='0.0',
      description='Library for processing data on PySpark',
      long_description=readme(),
      classifiers=[
        'License :: OSI Approved :: GNU License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Text Processing :: Linguistic',
      ],
      keywords='pyspark',
      url='https://github.com/Profitrollka/PySpark',
      author='Alena Kapustyan',
      author_email='alenocka.1989@gmail.com',
      license='GNU',
      packages=['pysp_lib'],
      install_requires=[
          'pyspark',
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['funniest-joke=funniest.command_line:main'],
      },
      include_package_data=True,
      zip_safe=False)
