library(arrow)
library(tidyverse)

# Script to create admission_diagnosis table for MIMIC-CLIF

output_path <- "/Users/cdiaz/Desktop/SRP/SRP SOFA/code/lookup tables"

d_items <- read_parquet("/Users/cdiaz/Documents/R/Data/mimic-iv-3.1/parquet/d_items.parquet")

# Mapping
d_icd_diagnoses <- read.csv("/Users/cdiaz/Documents/R/Data/mimic-iv-3.1/d_icd_diagnoses.csv")

# Diagnoses
icd_diagnoses <- read.csv("/Users/cdiaz/Documents/R/Data/mimic-iv-3.1/diagnoses_icd.csv")

icd_diagnoses <- icd_diagnoses %>%
  rename(hospitalization_id = hadm_id) %>% 
  left_join(d_icd_diagnoses %>% select(icd_code, long_title), by = "icd_code")

admission_diagnosis <- icd_diagnoses %>% 
  rename(diagnostic_code = icd_code,
         diagnosis_code_format = icd_version,
         patient_id = subject_id) %>% 
  mutate(diagnosis_code_format = ifelse(diagnosis_code_format == "9", "icd9", "icd10")) %>% 
  select(-long_title) %>% 
  distinct()

write_parquet(admission_diagnosis, paste0(output_path, "/clif_admission_diagnosis", ".parquet"))
  
  
  