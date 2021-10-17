from setuptools import setup


def readme():
    with open('README.md') as f:
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
      url='https://github.com/Profitrollka/PySpark/pysp_lib',
      author='Alena Kapustyan',
      author_email='alenocka.1989@gmail.com',
      license='GNU',
      packages=['pysp_lib'],
      python_requires='>=3.0, <4',
      install_requires=[
          'pyspark',
      ],
      include_package_data=True,
      zip_safe=False)
