import pandas as pd
import json

from data_cleaning.utils import load_json, save_json
from data_cleaning.sir import (
    convert_wwbakt_to_lims_sir_mic,
    find_sir_mic_variables_df,
    find_sir_mic_variables_dict,
)


def generate_rename_columns_json(df: pd.DataFrame) -> str:
    """
    Generates a JSON object with all column names,
    where replacement names are to be filled in.
    Leave empty to keep the name unchanged.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to generate the JSON object from.

    Returns
    -------
    json
        A JSON object with all column names.
    """
    columns = df.columns.values

    rename_dict = dict.fromkeys(columns, "")

    rename_json = json.dumps(rename_dict, ensure_ascii=False, indent=4)
    return rename_json


def generate_and_save_rename_columns_json(df: pd.DataFrame, file_path: str):
    """
    Generates a rename JSON from a DataFrame and saves it to a specified path.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to generate the JSON object from.
    file_path : str
        Path where the JSON file will be saved.
    """
    rename_json = generate_rename_columns_json(df)
    save_json(rename_json, file_path)


def rename_columns(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    Renames the variables in a DataFrame according to the rename file.
    """
    df = df.copy()
    rename_dict = load_json(path)
    columns_to_keep = [col for col in df.columns if rename_dict[col] != "remove"]
    df = df[columns_to_keep]
    df = df.rename(columns=lambda x: rename_dict[x] if rename_dict[x] != "" else x)
    return df


def rename_sir_mic_variables(
    sir_rename_path: str = None,
    antibiotics: str = "Type of antibiotics",
    sir_id: str = "Sir",
    mic_id: str = "MicSir",
) -> json:
    """ """
    # load the rename files
    rename_dict = load_json(sir_rename_path)
    rename_dict_antibiotics = rename_dict[antibiotics]

    # get the MIC and SIR variables. LIMS sir variables always have spaces in the name
    # TODO: Do this in a more general way
    sir_variables_no_space = [
        col for col in rename_dict_antibiotics.keys() if " " not in col
    ]
    # print(sir_variables_no_space)

    sir_variables, mic_variables = find_sir_mic_variables_dict(
        sir_variables_no_space, sir_id, mic_id
    )

    # do the conversion and check if a match can be found in the LIMS rename file
    # if a match is found, update the wwBakt rename file
    for col in mic_variables:
        new_value = convert_wwbakt_to_lims_sir_mic(col, mic_id)
        if new_value in rename_dict_antibiotics.keys():
            rename_dict_antibiotics[col] = new_value

    # same for SIR variables
    for col in sir_variables:
        new_value = convert_wwbakt_to_lims_sir_mic(col, sir_id)
        if new_value in rename_dict_antibiotics.keys():
            rename_dict_antibiotics[col] = new_value

    rename_dict[antibiotics] = rename_dict_antibiotics

    return json.dumps(rename_dict, ensure_ascii=False)


def generate_rename_values_json(df: pd.DataFrame, limit: int) -> json:
    """
    Generates a JSON file with all value names for variables with less than 'limit' number of unique values,
    where replacement names are to be filled in.
    Leave empty to keep the name unchanged.
    """
    d = dict()
    for col in sorted(df.select_dtypes(include=["object"]).columns):
        values = df[col].dropna().unique()
        if len(values) < limit:
            d[col] = {value: "" for value in sorted(values,key=lambda x: x.lower())}

    return json.dumps(d, ensure_ascii=False,indent=4)


def generate_and_save_rename_values_json(df: pd.DataFrame, limit: int, file_path: str):
    """
    Generates a rename JSON from a DataFrame and saves it to a specified path.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to generate the JSON object from.
    limit : int
        Maximum number of unique values for a variable to be included in the JSON object.
    file_path : str
        Path where the JSON file will be saved.
    """
    rename_json = generate_rename_values_json(df, limit)
    save_json(rename_json, file_path)


def rename_values(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    Renames the values in a DataFrame according to a JSON file.
    """
    renamed_df = df.copy()
    rename_dict = load_json(path)

    for var in rename_dict:
        renamed_df[var] = renamed_df[var].apply(
            lambda x: (
                rename_dict[var].get(x, x) if rename_dict[var].get(x, x) != "" else x
            )
        )

    return renamed_df
