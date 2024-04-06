""" This module has some common usefull general functions that are used in other submodules.
"""

import numpy as np
import pandas as pd


def cast_to_time(x: str, format: str = "%d/%m/%Y") -> pd.to_datetime:
    """ Cast a string or pd.Series to datetime using `pd.to_datetime` pandas function.
    The funcion string default format is dd/mm/yyyy.

    Args:
        x (str): String to be converted.

    Returns:
        pd.datetime: Pandas date time object.

    Examples:
        This function may be used to convert a single string or entire pandas
        series to a date time object. This first example uses the default date
        format to convert a string.

        >>> cast_to_time('03/12/2019')
        Timestamp('2017-09-23 00:00:00')

        If the converted string is not in the default format, the result is `np.nan`.
        >>> cast_to_time('2019-09-23')
        nan

        This third example apply a different formater to convert the string.

        >>> cast_to_time('2019-09-23', format='%Y-%m-%d')
        Timestamp('2019-09-23 00:00:00')

        It`s also possible to cast an entire pandas series to datetime:

        >>> data = pd.Series(['2013-08-01', '2014-07-02', '2015-06-03'])
        >>> cast_to_time(data, format='%Y-%m-%d')
        0   2013-08-01
        1   2014-07-02
        2   2015-06-03
        dtype: datetime64[ns]
    """
    try:
        return pd.to_datetime(x, format=format)
    except ValueError:
        return np.nan
    except TypeError:
        return np.nan


def cast_to_hour(x: str) -> pd.to_datetime:
    """Cast a string to hour using `pd.to_datetime` pandas function.
    The funcion string must be in the format dd/mm/yyyy 00:00:00.

    Args:
        x (str): String to be converted.

    Returns:
        pd.datetime: Pandas date time object.

    .. deprecated:: 0.01
       Use :func:`cast_to_time`
    """
    try:
        return pd.to_datetime(x, dayfirst=True, yearfirst=False)
    except ValueError:
        return np.nan
    except TypeError:
        return np.nan


def cast_to_int(x: str) -> int:
    """ Try to cast a string to an integer.

    Args:
        x (str): The string to be converted.

    Returns:
        int: The string as an integer.

    Examples:

        >>> cast_to_int('123')
        123
        >>> cast_to_int('3.14')
        3
        >>> cast_to_int('abc')
        nan
    """
    try:
        return int(float(x))
    except Exception:
        return np.nan


def cast_to_float(x: str) -> float:
    """ Try to cast a string to float.

    Args:
        x (str): The string to be cast.

    Returns:
        float: The string as a float.

    Examples:

        >>> cast_to_float('3.14')
        3.14
        >>> cast_to_float('abc')
        nan
    """
    try:
        return float(x)
    except ValueError:
        return np.nan


def get_len_string(value: str) -> int:
    """ Try getting the lenght of a string.

    Args:
        value (str): The string to get the lenght.

    Returns:
        int: The lenght of the string or np.nan

    Examples:

        >>> get_len_string('abcdefg')
        6
        >>> get_len_string(int)
        nan
    """
    try:
        return len(value)
    except TypeError:
        return np.nan


def replace_wild_characters(text: pd.Series) -> pd.Series:
    """Transform all characters of the given DataFrame.

    All characters are transformed to lower case, and special characters are removed.

    Args:
        text (pd.Series): The series to be treated (default: {None}).

    Returns:
        pd.Series: Pandas Series with characters replaced.

    Examples:
        >>> data = pd.Series(['MaÇã', 'pÉ', 'pêra', 'híFeN'])
        >>> replace_wild_characters(data)
        0     maca
        1       pe
        2     pera
        3    hifen
        dtype: object
    """
    text_lower = text.str.lower()
    text_lower = text_lower.str.strip()
    chars_to_replace = {
        "à": "a",
        "ä": "a",
        "ã": "a",
        "â": "a",
        "á": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "õ": "o",
        "ó": "o",
        "ô": "o",
        "ç": "c",
    }
    for k, v in chars_to_replace.items():
        text_lower = text_lower.str.replace(k, v)
    return text_lower


def replace_wild_characters_str(text: str) -> str:
    """Transform all characters of the given string.

    All characters are transformed to lower case, and special characters are removed.

    Args:
        text (str): The string to be treated (default: {None}).

    Returns:
        str: string with characters replaced.

    Examples:
        >>> txt = 'MaÇã'
        >>> replace_wild_characters_string(txt)
        maca
    """
    text_lower = text.lower()
    chars_to_replace = {
        "à": "a",
        "ä": "a",
        "ã": "a",
        "â": "a",
        "á": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "õ": "o",
        "ó": "o",
        "ô": "o",
        "ç": "c",
    }
    for k, v in chars_to_replace.items():
        text_lower = text_lower.replace(k, v)
    return text_lower
