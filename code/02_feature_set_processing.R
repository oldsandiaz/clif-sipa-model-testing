#################################################
### Feature set creation for model development
#################################################

# Load necessary libraries
library(arrow)
library(tidyverse)
library(stringr)

# Clear env
rm(list = ls())

# Load data
data <- read_parquet("/Users/cdiaz/Desktop/SRP/SRP SOFA/output/intermediate/sofaclif_cohort.parquet")

################
### Summary functions for SOFA variables
################

# Variables to create the SOFA score
sofa_variables <- c("max_creatinine", 
                    "min_plt_count", 
                    "p_f_imputed",
                    "dobutamine",
                    "dobutamine_ne_eq",
                    "phenylephrine",
                    "phenylephrine_ne_eq",
                    "dopamine",
                    "dopamine_ne_eq",
                    "epinephrine",
                    "epinephrine_ne_eq",
                    "gcs_total",
                    "max_bilirubin",
                    "min_map",
                    "s_f",
                    "norepinephrine")

# Summary functions to use for each SOFA variable
worst_functions <- list(
  max_creatinine = max,
  min_plt_count = min,
  p_f_imputed = min,
  dobutamine = max,
  dobutamine_ne_eq = max,
  phenylephrine = max,
  phenylephrine_ne_eq = max,
  dopamine = max,
  dopamine_ne_eq = max,
  epinephrine = max,
  epinephrine_ne_eq = max,
  gcs_total = min,
  max_bilirubin = max,
  min_map = min,
  s_f = min,
  norepinephrine = max
)

# For each hospitalization_id, summarize the SOFA variables from
# window_start to life_support_start

## First, pull all data within the window_start to life_support_start timeframe

pre_ls_data <- data %>% 
  mutate(
    # Create time string with leading zero for hour
    time_str = str_c(str_pad(meas_hour, 2, pad = "0"), "00", "00", sep = ":"),
    # Concatenate date and time for datetime_str
    datetime_str = str_c(meas_date, time_str, sep = " "),
    # Now parse as POSIXct
    meas_dttm = as.POSIXct(datetime_str, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
  ) %>% 
  select(-time_str, -datetime_str)

# Filter for the relevant time window
pre_ls_data <- pre_ls_data %>% 
  filter(meas_dttm >= window_start & meas_dttm < life_support_start)

# Now summarize the SOFA variables for each hospitalization_id
df_sum <- pre_ls_data %>%
  group_by(hospitalization_id) %>%
  summarise(across(
    names(worst_functions),
    ~ worst_functions[[cur_column()]](., na.rm = TRUE)
  ))

# Rename the columns to match SOFA variable names
df_sum <- df_sum %>% 
  rename(
    creatinine = max_creatinine,
    platelets = min_plt_count,
    p_f = p_f_imputed,
    dobutamine = dobutamine,
    phenylephrine = phenylephrine,
    dopamine = dopamine,
    gcs = gcs_total,
    bilirubin = max_bilirubin,
    map = min_map,
    s_f = s_f,
    norepinephrine = norepinephrine
  )

# Clean up the data: replace infinite values with NA and filter out rows
df_sum <- df_sum %>%
  mutate(across(
    -hospitalization_id,
    ~ ifelse(is.infinite(.), NA_real_, .)
  ))

# Remove rows where all SOFA variables are NA
df_sum_clean <- df_sum %>%
  filter(!if_all(-hospitalization_id, is.na))

# Keep all columns from data df except sofa_variables
data_clean <- data %>%
  select(hospitalization_id, zipcode_nine_digit,
         census_block_group_code, ethnicity_category,
         age_at_admission, zipcode_five_digit, 
         race_category, sex_category,
         in_hospital_mortality) %>% 
  distinct()

# Join the summarized SOFA variables with the cleaned data 
final_data <- df_sum_clean %>%
  left_join(data_clean, by = "hospitalization_id")

####################
### COMPUTE SOFA SCORE FOR EACH HOSPITALIZATION
####################

# Define the SOFA score calculation function
compute_sofa_score <- function(row) {
  
  # Extract the relevant variables from the row
  p_f <- row$p_f
  s_f <- row$s_f
  platelets <- row$platelets
  bilirubin <- row$bilirubin
  map <- row$map
  dopamine <- row$dopamine
  dobutamine <- row$dobutamine
  norepinephrine <- row$norepinephrine
  epinephrine <- row$epinephrine
  gcs <- row$gcs
  creatinine <- row$creatinine
  
  # Respiration (PaO2/FiO2 or SaO2/FiO2 ratio)
  resp <- NA
  if (!is.na(p_f)) {
    if (p_f >= 400) 0
    else if (p_f >= 300) 1
    else if (p_f >= 200) 2
    else if (p_f >= 100) 3
    else 4
  } else if (!is.na(s_f)) {
    if (s_f > 301) 0
    else if (s_f >= 221 && s_f <= 301) 1
    else if (s_f >= 142 && s_f <= 220) 2
    else if (s_f >= 67 && s_f <= 141) 3
    else if (s_f < 67) 4
  }
  
  # Coagulation (platelet count)
  coag <- if (!is.na(platelets)) {
    if (platelets >= 150) 0
    else if (platelets >= 100) 1
    else if (platelets >= 50) 2
    else if (platelets >= 20) 3
    else 4
  } else {
    NA
  }
  
  # Liver (bilirubin, mg/dl)
  liver <- if (!is.na(bilirubin)) {
    if (bilirubin < 1.2) 0
    else if (bilirubin < 2.0) 1
    else if (bilirubin < 6.0) 2
    else if (bilirubin < 12.0) 3
    else 4
  } else {
    NA
  }
  
  # Cardiovascular
  cv <- 0
  # 1: MAP < 70 mmHg
  if (!is.na(map) && map < 70) cv <- 1
  # 2: Dopamine <= 5 or any dobutamine
  if ((!is.na(dopamine) && dopamine > 0 && dopamine <= 5) || (!is.na(dobutamine) && dobutamine > 0)) cv <- 2
  # 3: Dopamine > 5 or norepinephrine <= 0.1 or epinephrine <= 0.1
  if ((!is.na(dopamine) && dopamine > 5) ||
      (!is.na(norepinephrine) && norepinephrine > 0 && norepinephrine <= 0.1) ||
      (!is.na(epinephrine) && epinephrine > 0 && epinephrine <= 0.1)) cv <- 3
  # 4: Dopamine > 15 or norepinephrine > 0.1 or epinephrine > 0.1
  if ((!is.na(dopamine) && dopamine > 15) ||
      (!is.na(norepinephrine) && norepinephrine > 0.1) ||
      (!is.na(epinephrine) && epinephrine > 0.1)) cv <- 4
  
  # CNS (GCS)
  cns <- if (!is.na(gcs)) {
    if (gcs == 15) 0
    else if (gcs >= 13) 1
    else if (gcs >= 10) 2
    else if (gcs >= 6) 3
    else 4
  } else {
    NA
  }
  
  # Renal (creatinine, mg/dl)
  renal <- if (!is.na(creatinine)) {
    if (creatinine < 1.2) 0
    else if (creatinine < 2.0) 1
    else if (creatinine < 3.5) 2
    else if (creatinine < 5.0) 3
    else 4
  } else {
    NA
  }
  
  total_score <- sum(c(resp, coag, liver, cv, cns, renal), na.rm = TRUE)
  return(total_score)
}

final_data <- final_data %>%
  rowwise() %>%
  mutate(sofa_score = compute_sofa_score(cur_data()))
  

# Export the final data as a parquet 
write_parquet(final_data, 
               "/Users/cdiaz/Desktop/SRP/SRP SOFA/output/intermediate/sipa_features.parquet")

