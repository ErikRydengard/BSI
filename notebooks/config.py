class Config:

    BASE_DIR = "/Volumes/KINGSTON"
    DATA_PATH = f"{BASE_DIR}/converted_parquet"
    UTIL_DATA_PATH = f"{BASE_DIR}/util_data"
    RENAME_DIR = "../rename_files/microbiology"
    STORE_PATH = f"{BASE_DIR}/BSI_TTP_Databas/cleaned_data"
    NEW_TIMES_Path = f"{DATA_PATH}/sample_times.parquet"

   # Store microbiology data
    STORE_MICROBIOLOGY_PATH = f"{STORE_PATH}/microbiology"
    STORE_REFERENCE_DATA_PATH = f"{STORE_PATH}/reference_data"
    STORE_DIALYSIS_DATA_PATH = f"{STORE_PATH}/dialysis"
    STORE_DIAGNOSIS_DATA_PATH = f"{STORE_PATH}/diagnosis"
    STORE_CEILING_DATA_PATH = f"{STORE_PATH}/ceiling"
    STORE_ADMINSTRATION_OF_ANTIBIOTICS_DATA_PATH = f"{STORE_PATH}/administration_antibiotics"
    STORE_LABORATORY_DATA_PATH = f"{STORE_PATH}/laboratory"
    STORE_MEDICINE_DATA_PATH = f"{STORE_PATH}/medicine"
    STORE_ORBIT_DATA_PATH = f"{STORE_PATH}/orbit"
    STORE_OUTCOME_DATA_PATH = f"{STORE_PATH}/outcome"


    # Path to util data
    REFERENCE_DATA_PATH = f"{STORE_PATH}/reference_data.parquet"
    INFECTION_CODES_PATH = f"{UTIL_DATA_PATH}/ICD_koder_huvudanalys.xlsx"

    # Paths to the data
    # microbiology
    WWBAKT_PATH = f"{DATA_PATH}/MIKROBIOLOGI_Sammanstallning_Blododlingsfynd_2011-2021_wwBakt.parquet"
    LIMS_PATH = f"{DATA_PATH}/MIKROBIOLOGI_Sammanstallning_Blododlingsfynd_2021_2023_LIMS.parquet"

    # Diagnosis and hospitalisation
    RSVD_OVA_DIAGNOSIS_PATH = (
        f"{DATA_PATH}/RSVD_OVA_Diagnoser_365dagarInnanBaseline.parquet"
    )
    RSVD_SVA_DIAGNOSIS_PATH = (
        f"{DATA_PATH}/RSVD_SVA_Diagnoser_365dagarInnanBaseline.parquet"
    )
    RSVD_CANCER_DIAGNOSIS_PATH = f"{DATA_PATH}/RSVD_OVA_SVA_Cancerdiagnoser.parquet"

    RSVD_SVA_HOSPITALISATION_PATH = (
        f"{DATA_PATH}/RSVD_SVA_365ForeOchEfterBaseline.parquet"
    )

    MELIOR_OVA_EPIKRIS_PATH = (
        f"{DATA_PATH}/Melior_OppenVardtillfalle_PatientDiagnos_Epikris.parquet"
    )
    MELIOR_SVA_EPIKRIS_PATH = f"{BASE_DIR}/Uttag_v2/Melior_SlutenVardtillfalle_PatientDiagnos_Epikris_v2.parquet"

    MELIOR_OV_PATH = f"{DATA_PATH}/Melior_OppenVardtillfalle.parquet"
    MELIOR_SV_PATH = f"{DATA_PATH}/Melior_SlutenVardtillfalle_v2.parquet"

    MELIOR_SV_MULTI = f"{DATA_PATH}/Melior_SlutenVardtillfalle_Flerval_v2.parquet"
    MELIOR_OV_MULTI = f"{DATA_PATH}/Melior_OppenVardtillfalle_Flerval.parquet"

    MELIOR_OV_ANTIBIOTIC_ORDINATION_PATH =  f"{DATA_PATH}/Melior_OppenVardtillfalle_OrdinationUtdelning_J01.parquet"
    MELIOR_SV_ANTIBIOTIC_ORDINATION_PATH = f"{DATA_PATH}/Melior_SlutenVardtillfalle_OrdinationUtdelning_J01_v2.parquet"

    # Medicine
    MEDICINE_PRESCRIPTION = f"{DATA_PATH}/LakemforskrAllaPerioder_365dagarInnanBaseline.parquet"

    # laboratory and vitals
    LABORATORY_PATH = f"{DATA_PATH}/Melior_SlutenVardtillfalle_Labanalyssvar_v2.parquet"
    VITALS_PATH = f"{DATA_PATH}/Melior_SlutenVardtillfalle_Tal_v2.parquet"

    # Dialysis
    DIALYSIS_PATH = f"{DATA_PATH}/Melior_Enval_Dialys.parquet"

    # ORBIT
    ORBIT4_PATH = f"{DATA_PATH}/Orbit4_SamtligaOp180dagarInnanBaseline.parquet"
    ORBIT5_PATH = f"{DATA_PATH}/Orbit5_SamtligaOp180dagarInnanBaseline.parquet"
    ORBIT4_FOREIGN_OBJECT_PATH = (
    f"{DATA_PATH}/Orbit4_SamtligaOpInsattningFrammandeMaterial.parquet"
    )
    ORBIT5_FOREIGN_OBJECT_PATH = (
    f"{DATA_PATH}/Orbit5_SamtligaOpInsattningFrammandeMaterial.parquet"
    )

    # mortality
    MORTALITY_PATH = f"{DATA_PATH}/RS_Pat_Alias_Kon_Avliden.parquet"

    # Rename files
    RENAME_FILES_PATH_MICROBIOLOGY = "../rename_files/microbiology"
    RENAME_FILES_PATH_HOSPITALISATION = "../rename_files/hospitalisation"
    RENAME_FILES_PATH_DIAGNOSIS = "../rename_files/diagnosis"
    RENAME_FILES_PATH_CEILING = "../rename_files/ceiling"
    RENAME_FILES_ADMINSTRATION_ANTIBIOTICS = "../rename_files/administration_antibiotics"
    RENAME_FILES_DIALYSIS = "../rename_files/dialysis"
    RENAME_FILES_MEDICINE = "../rename_files/medicine"
    RENAME_FILES_LABORATORY = "../rename_files/laboratory"
    RENAME_FILES_ORBIT = "../rename_files/orbit"
    # processed data
    MICROBIOLOGY_DEDUB_PATH = f"{STORE_MICROBIOLOGY_PATH}/microbiology_dedub.parquet"
    MICROBIOLOGY_PATH = f"{STORE_MICROBIOLOGY_PATH}/microbiology_without_contaminants.parquet"
    REFERENCE_DATA_PATH = f"{STORE_REFERENCE_DATA_PATH}/reference_data.parquet"
    IMMUNSUPPRESSION_EPISODES = f"{STORE_PATH}/immunosuppression_episodes.parquet"