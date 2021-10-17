# pysp_lib

pysp_lib is a library for processing data on Pyspark.

## Installation

You need to use the proper git URL:

pip install git+https://github.com/Profitrollka/PySpark/pysp_lib.git


## Usage

```python

# check.py

from pysp_lib.check import *

# Filter non-filled INN, create a new column with the correct INN (without letters, with zero added at the beginning of the INN, if the length of the INN is not correct), retern Pyspark DataFrame with extra column containing fixed INN.
df_transform = transform_inn_col(pySpark_df, 'inn')

# Check validity of INN by counting check sum, return Pyspark DataFrame with extra column containing flag validity of INN.
test_check_inn_func_fixed_inn = check_inn(test_transform_func, 'inn_fixed')

#More information about usage in the file Test_checkpy.ipynb

```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
(https://choosealicense.com/licenses/lgpl-3.0/)
