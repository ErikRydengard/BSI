import pandas as pd
import json


def save_json(j: json, path: str):
    """
    Saves the json to the specified path.
    """
    try:
        with open(path, "x") as file:
            file.write(j)
            print(f"File saved to {path}")
    except FileExistsError:
        print(f"File {path} already exists.")


def load_json(path: str) -> dict:
    """
    Load json from the specified path.
    """
    with open(path, "r") as file:
        rename_dict = json.load(file)
    return rename_dict


def convert_variable_to_snakecase(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the column names in a DataFrame to snake_case.
    """
    df = df.copy(deep=True)
    columns = df.columns
    columns = [col.lower().replace(" ", "_") for col in columns]
    df.columns = columns
    return df


def generate_mapping_alias_to_baseline(
    df: pd.DataFrame, alias_col: str, baseline: str, cols_to_sort: list
) -> dict:
    """
    Function that generates a mapping a dictionary that maps a alias id to a baseline time.


    Example:
    generate_mapping_alias_to_baseline(df = microbiology_data_renamed,
                                       alias_col="episode_id",
                                       cols_to_sort=["patient_id", "date_of_sampling"],
                                       baseline="date_of_sampling")
    """
    df = df.copy()

    df = df.sort_values(by=cols_to_sort)

    mapping = df[[alias_col, baseline]]

    mapping = mapping.drop_duplicates()

    # Group by the alias column, the base line will be the minimum date
    baselines = mapping.groupby(alias_col).min().reset_index()

    mapping_dict = dict(zip(baselines[alias_col], baselines[baseline].astype(str)))

    mapping_json = json.dumps(mapping_dict)

    return mapping_json


def generate_mapping_alias_to_alias(df: pd.DataFrame, aliases: list) -> json:
    """
    Function that generates a mapping a dictionary that maps a alias id to another alias id.

    """
    df = df.copy()

    mapping_dict = dict(zip(df[aliases[0]].astype(str), df[aliases[1]].astype(str)))

    mapping_json = json.dumps(mapping_dict)

    return mapping_json


def generate_mapping_alias_to_alias_dict(df: pd.DataFrame, from_alias:str, to_alias:str) -> dict:
    """
    Function that generates a mapping a dictionary that maps a alias id to another alias id.

    """
    df = df.copy()

    mapping_dict = dict(zip(df[from_alias].astype(str), df[to_alias].astype(str)))

    return mapping_dict
