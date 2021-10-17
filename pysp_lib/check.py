from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from operator import add
from functools import reduce


def transform_inn_col(dataframe, inn_col):
    """Function filters non-filled INN, creates a new column with the correct INN (without letters, with zero added
     at the beginning of the INN, if the length of the INN is not correct.)
    :param dataframe: Pyspark DataFrame for processing
    :param inn_col: column name contains INN
    :return Pyspark DataFrame.
    """
    inn_col_fixed = '{}_fixed'.format(inn_col)
    dataframe_filtered = dataframe.filter(f.col(inn_col).isNotNull()) \
        .withColumn('{}_tmp'.format(inn_col), f.regexp_replace(inn_col, r'\D+', '')) \
        .withColumn(inn_col_fixed,
                    f.when((f.length('{}_tmp'.format(inn_col)) == 9)
                           | (f.length('{}_tmp'.format(inn_col)) == 11)
                           , f.concat(f.lit('0'), f.col('{}_tmp'.format(inn_col))))
                    .otherwise(f.col('{}_tmp'.format(inn_col)))
                    ) \
        .drop(f.col('{}_tmp'.format(inn_col)))
    return dataframe_filtered


def check_inn(dataframe, inn_col):
    """Function checks validity of INN by counting check sum.
    :param dataframe: Pyspark DataFrame for processing.
    :param inn_col: column name contains INN.
    :return Pyspark DataFrame with extra column containing flag validity of INN.
    """

    coefs_dict = {'10': [2, 4, 10, 3, 5, 9, 4, 6, 8, 0],
                  '121': [7, 2, 4, 10, 3, 5, 9, 4, 6, 8],
                  '122': [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
                  }

    def inn_check_sum(coefs):
        """Function returns a digit, resulting from the addition of products of all digits and corresponding
        coefficients.
        :param coefs: List of coefficients.
        """
        return reduce(add, [f.substring(f.col(inn_col), index + 1, 1).cast(IntegerType()) * coef for (index, coef)
                            in enumerate(coefs)]) % 11 % 10

    inn_length_10 = (
            inn_check_sum(coefs_dict['10']) == f.substring(f.col(inn_col), -1, 1).cast(IntegerType()))
    inn_length_12_1 = (
            inn_check_sum(coefs_dict['121']) == f.substring(f.col(inn_col), -2, 1).cast(IntegerType()))
    inn_length_12_2 = (
            inn_check_sum(coefs_dict['122']) == f.substring(f.col(inn_col), -1, 1).cast(IntegerType()))
    inn_length_12 = inn_length_12_1 & inn_length_12_2
    check_sum_inn_10 = (f.length(f.col(inn_col)) == 10) & (f.col(inn_col) != '0000000000') & inn_length_10
    check_sum_inn_12 = (f.length(f.col(inn_col)) == 12) & (f.col(inn_col) != '000000000000') & inn_length_12
    check_sum_inn = check_sum_inn_10 | check_sum_inn_12

    return dataframe.withColumn('{}_valid_flag'.format(inn_col), f.when(check_sum_inn, f.lit(1)).otherwise(f.lit(0)))
