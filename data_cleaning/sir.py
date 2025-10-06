import pandas as pd
import json


def find_sir_mic_variables_df(
    df: pd.DataFrame, sir_id: str = "Sir", mic_id: str = "Mic"
) -> list:
    """
    Finds all variables containing SIR and MIC values.
    """
    sir_mic_variables = [
        col
        for col in df.columns
        if sir_id.lower() in col.lower() or mic_id.lower() in col.lower()
    ]
    return sir_mic_variables


def find_sir_mic_variables_dict(
    rename_list: dict, sir_id: str = "Sir", mic_id: str = "Mic"
) -> tuple:

    sir_mic_variables = [
        col
        for col in rename_list
        if sir_id.lower() in col.lower() or mic_id.lower() in col.lower()
    ]
    sir_variables = [
        col
        for col in rename_list
        if sir_id.lower() in col.lower() and mic_id.lower() not in col.lower()
    ]
    mic_variables = list(set(sir_mic_variables) - set(sir_variables))

    return sir_variables, mic_variables


def separate_sir_mic_data(df: pd.DataFrame, id_variables:list= []) -> tuple:
    """
    Separates the SIR data from the rest of the data. Returns a DataFrame with the separated SIR data.
    """
    df = df.copy()
    sir_mic_variables = find_sir_mic_variables_df(df)
    sir_mic_data = df[id_variables + sir_mic_variables]

    df = df.drop(columns=sir_mic_variables)
    return df, sir_mic_data


def convert_wwbakt_to_lims_sir_mic(name: str, id: str) -> str:
    if "mic" in id.lower():
        return name.replace(id, "").strip() + " MIC"
    else:
        return "SIR " + name.replace(id, "").strip()
