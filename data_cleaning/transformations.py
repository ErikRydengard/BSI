import pandas as pd


def remove_redundant_decimals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes redundant zeros and dots from values in the DataFrame. Only applied to columns of numbers
    without decimals, while handling NaN values properly.
    """
    converted_df = df.copy()

    df_numbers = converted_df.select_dtypes(include=["number"]).copy()

    for col in df_numbers.columns:
        # Check if all non-NaN values in the column can be converted to integers without loss
        if (df_numbers[col].dropna() == df_numbers[col].dropna().astype(int)).all():
            # int cant handle NaN values, Int64 is needed
            converted_df[col] = df_numbers[col].astype("Int64")

    return converted_df


def convert_to_datetime(
    df: pd.DataFrame, columns_to_convert: list = []
) -> pd.DataFrame:
    """
    Converts columns in a DataFrame to datetime where applicable.

    The function identifies columns that either:

    - Contain the word 'date' in their column name.
    - Have a data type related to date-like objects.

    It tries multiple date formats and reports columns that fail the conversion.

    Parameters:
    df : pd.DataFrame
        The DataFrame with columns that may contain dates.

    Returns:
    pd.DataFrame
        The DataFrame with applicable columns converted to datetime.
    """
    # List of common date formats to try
    date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%d-%m-%Y", "%m/%d/%Y"]

    df = df.copy()

    if len(columns_to_convert) == 0:
        columns = df.columns
    else:
        columns = columns_to_convert

    for col in columns:
        # Check if the column name contains 'date' or if the dtype suggests it's a date
        if "date" in col.lower() or pd.api.types.is_object_dtype(df[col]):
            for date_format in date_formats:
                try:
                    df[col] = pd.to_datetime(
                        df[col], format=date_format, errors="raise"
                    )
                    # remove tz
                    df[col] = df[col].dt.tz_localize(None)
                    print(f"Column '{col}' successfully converted to datetime.")
                    break  # Stop after the first successful conversion
                except (ValueError, TypeError):
                    continue  # Try the next date format if the current one fails

    return df


def convert_to_datetime_with_keyword(
    df: pd.DataFrame, columns_to_convert: list = [],keyword: str = "date"
) -> pd.DataFrame:
    """
    Converts columns in a DataFrame to datetime where applicable.

    The function identifies columns that either:

    - Contain the word 'date' in their column name.
    - Have a data type related to date-like objects.

    It tries multiple date formats and reports columns that fail the conversion.

    Parameters:
    df : pd.DataFrame
        The DataFrame with columns that may contain dates.

    Returns:
    pd.DataFrame
        The DataFrame with applicable columns converted to datetime.
    """
    # List of common date formats to try
    date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%d-%m-%Y", "%m/%d/%Y"]

    df = df.copy()

    if len(columns_to_convert) == 0:
        columns = df.columns
    else:
        columns = columns_to_convert

    for col in columns:
        # Check if the column name contains 'date' or if the dtype suggests it's a date
        if keyword in col.lower() or pd.api.types.is_object_dtype(df[col]):
            for date_format in date_formats:
                try:
                    df[col] = pd.to_datetime(
                        df[col], format=date_format, errors="raise"
                    )
                    # remove tz
                    df[col] = df[col].dt.tz_localize(None)
                    print(f"Column '{col}' successfully converted to datetime.")
                    break  # Stop after the first successful conversion
                except (ValueError, TypeError):
                    continue  # Try the next date format if the current one fails

    return df


def reshape_to_long_format(
    df: pd.DataFrame,
    id_vars: list,
    value_vars: list,
    var_name: str,
    value_name: str,
) -> pd.DataFrame:
    """
    Converts a DataFrame from wide to long format. \n
    Parameters: \n
    id_vars: list of columns to keep as identifiers \n
    value_vars: list of columns to unpivot \n
    var_name: name of the column to store the variable names
    """
    # function for unpivoting the data
    df = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
    )
    df = df.dropna()
    return df



#TODO WORK IN PROGRESS, NOT FUNCTIONAL
# def add_care_limitation(df: pd.DataFrame,df_oppenVardtillfalle: pd.DataFrame, df_fritext: pd.DataFrame, 
#                         sample_id_col: str = 'sample_id', patient_id_col: str = 'patient_id',
#                         vardtillfalle_col: str = 'VardtillfalleAlias',
#                         fritext_col: str = 'Fritext_Varde',
#                         fritext_date_col: str = 'Fritext_ModifieradDatum') -> pd.DataFrame:
#     df = df.copy()
#     df_oppenVardtillfalle = df_oppenVardtillfalle.copy()
#     df_fritext = df_fritext.copy()

#     df_fritext_added_id = df_fritext.merge(
#         df_oppenVardtillfalle[[sample_id_col, patient_id_col, vardtillfalle_col]],
#         how='left',
#         on=vardtillfalle_col
#     )

#     df_added_fritext = df.merge(df_fritext_added_id[[sample_id_col, fritext_col, fritext_date_col]], 
#                                      how='left', on=sample_id_col, indicator=False)
    
    
#     return df_added_fritext