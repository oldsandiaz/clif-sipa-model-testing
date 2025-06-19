# Cohort identification script for inpatient admissions

# Load required libraries
library(knitr)
library(here)
library(tidyverse)
library(arrow)
library(gtsummary)
library(stringr)
library(duckdb)
library(dtplyr)
library(data.table)

# DuckDB Helper function
read_data <- function(file_path) {
  if (grepl("\\.csv$", file_path)) {
    return(read.csv(file_path))
  } else if (grepl("\\.parquet$", file_path)) {
    return(arrow::read_parquet(file_path))
  } else if (grepl("\\.fst$", file_path)) {
    return(fst::read.fst(file_path))
  } else {
    stop("Unsupported file format")
  }
}

# DuckDB Writing Function
write_files_to_duckdb <- function(files, tables_location, file_type, con) {
  for (file_name in files) {
    file_path <- file.path(tables_location, paste0(file_name, file_type))
    data <- as.data.frame(read_data(file_path))
    table_name <- tools::file_path_sans_ext(file_name)
    duckdb::dbWriteTable(con, table_name, data, overwrite = TRUE)
    message("Wrote table: ", table_name, " from file: ", file_name)
    rm(data)
  }
}

# Function to track N for different slices

cohort_table <- data.frame(Reason = character(), N = integer(), stringsAsFactors = FALSE)

cohort_tracking <- function(exclusion_reason, current_data, cohort_table){
  n <- as.numeric(n_distinct(current_data$hospitalization_id))
  new_row <- data.frame(Reason = exclusion_reason, N = n)
  bind_rows(cohort_table, new_row)
}


# Specify inpatient cohort parameters

output_path <- "/Users/cdiaz/Desktop/SRP/SRP SOFA/output/intermediate"

## Date range
start_date <- "2020-01-01"
end_date <- "2021-12-31"

# Specify required CLIF tables

# Tables that should be set to TRUE for this project
true_tables <- list("clif_medication_admin_continuous", "clif_patient",
                    "clif_hospitalization", "clif_adt", "clif_respiratory_support",
                    "clif_labs", "clif_vitals", "clif_patient_assessments")


## Specify CLIF table location in your repository

# Load the configuration utility
source("utils/config.R")

# Access configuration parameters
site_name <- config$site_name
tables_path <- config$tables_path
file_type <- config$file_type

# Print the configuration parameters
print(paste("Site Name:", site_name))
print(paste("Tables Path:", tables_path))
print(paste("File Type:", file_type))

# WRITE TO DUCKDB DATABASE NAMED "con"
con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
write_files_to_duckdb(true_tables, tables_path, file_type, con)

# Generate a list of cohort IDs

# First slice: Exclusion criteria: Age > 18
hospitalization <- tbl(con, "clif_hospitalization") %>%
  select(hospitalization_id, patient_id, admission_dttm, discharge_dttm, age_at_admission,
         discharge_name, discharge_category, zipcode_nine_digit, 
         zipcode_five_digit, census_block_code) %>% 
  filter(discharge_dttm >= admission_dttm) %>% 
  filter(!is.na(discharge_dttm) & !is.na(admission_dttm))%>%
  filter(age_at_admission >= 18) %>% 
  collect()

hosp_ids <- hospitalization %>% 
  select(hospitalization_id)

# Save to cohort tracking table
cohort_table <- cohort_tracking("Age >= 18", hosp_ids, cohort_table)
cohort_table

# Second slice: find all hospitalization IDs associated with an ICU or Ward stay ONLY
adt_adults <- hosp_ids %>%
  inner_join(tbl(con, "clif_adt"), by = "hospitalization_id", copy = TRUE) %>%
  select(hospitalization_id, location_category) %>%
  distinct() %>%
  mutate(count = 1) %>% 
  pivot_wider(names_from = location_category, 
              values_from = count, values_fill = list(count = 0)) %>% 
  filter(icu == 1 | ward == 1) %>% 
  select(hospitalization_id) %>% 
  distinct()

hospitalization <- adt_adults %>% 
  left_join(hospitalization, by = "hospitalization_id")

hosp_ids <- hospitalization %>% 
  select(hospitalization_id)

# Save to cohort tracking table
cohort_table <- cohort_tracking("ICU and Wards", hosp_ids, cohort_table)
cohort_table


# Free up memory
rm(adt_adults)

# Get first and last vital times to get hourly timestamps for hospitalization
## Doing this by minimum and maximum vital time
vital_min_max_time <- tbl(con, "clif_vitals") %>% 
  inner_join(hosp_ids, copy = TRUE) %>% 
  group_by(hospitalization_id) %>% 
  summarise(
    min_dttm = min(recorded_dttm),
    max_dttm = max(recorded_dttm),
    .groups = "drop"
  ) %>%
  collect()

# Create a large dataframe that will store hourly data for each hospitalization ID

hosp_by_hour <- vital_min_max_time %>% 
  rowwise() %>%
  mutate(txnDt = list(seq.POSIXt(min_dttm, max_dttm, by = "hour"))) %>%
  unnest(txnDt) %>%
  mutate(meas_hour = hour(txnDt),
         meas_date = as.Date(txnDt)) %>%
  select(hospitalization_id, txnDt, meas_date, meas_hour) %>%
  arrange(hospitalization_id, txnDt, meas_date, meas_hour) %>%
  select(hospitalization_id, meas_date, meas_hour)

# Bring in RESPIRATORY SUPPORT data
## Remove CPAP AND (fio2_set NAs or fio2_set < .3)
## Remove fio2_set < .21 or fio2_set > 1 and replace with NAs
## Replace device name nulls with best guess of device name based on values
## Replace fio2_set NAs based on liters per minute (LPM) for nasal cannula
## Create recorded hours columns
## Rank all devices

resp_support <- tbl(con, "clif_respiratory_support") %>%
  inner_join(hosp_ids, by = "hospitalization_id", copy = TRUE) %>%
  select(hospitalization_id, recorded_dttm, mode_category, 
         device_category, lpm_set, fio2_set, peep_set, tidal_volume_set) %>%  
  collect() %>% 
  filter(!(device_category == "CPAP" & (is.na(fio2_set) | fio2_set < 0.3))) %>% 
  mutate(fio2_set_clean = ifelse(fio2_set < 0.21 | fio2_set > 1, NA, fio2_set))

resp_support <- resp_support %>% 
  mutate(device_category_2 = case_when(
    !is.na(device_category) ~ device_category,
    mode_category %in% c("SIMV", "Pressure-regulated Volume Control", "Assist Control-Volume Control") ~ "Vent",
    is.na(device_category) & fio2_set_clean == 0.21 & is.na(lpm_set) & is.na(peep_set) & is.na(tidal_volume_set) ~ "Room Air",
    is.na(device_category) & is.na(fio2_set_clean) & lpm_set == 0 & is.na(peep_set) & is.na(tidal_volume_set) ~ "Room Air",
    is.na(device_category) & is.na(fio2_set_clean) & lpm_set <= 20 & lpm_set > 0 & is.na(peep_set) & is.na(tidal_volume_set) ~ "Nasal Cannula",
    is.na(device_category) & is.na(fio2_set_clean) & lpm_set > 20 & is.na(peep_set) & is.na(tidal_volume_set) ~ "High Flow NC",
    device_category == "Nasal Cannula" & is.na(fio2_set_clean) & lpm_set > 20 ~ "High Flow NC",
    TRUE ~ device_category # Keep original device_category if no condition is met
  ))

## Try to fill in FiO2 based on LPM 
fio2_lookup <- c(
  `1` = 0.24, `2` = 0.28, `3` = 0.32, `4` = 0.36, `5` = 0.40,
  `6` = 0.44, `7` = 0.48, `8` = 0.52, `9` = 0.56, `10` = 0.60
)

resp_support <- resp_support %>%
  mutate(fio2_approx = case_when(
    !is.na(fio2_set_clean) ~ fio2_set,
    is.na(fio2_set_clean) & device_category_2 == "Room Air" ~ 0.21,
    is.na(fio2_set_clean) & device_category_2 == "Nasal Cannula" ~ (0.21 + (0.04 * lpm_set)),
    is.na(fio2_set_clean) & device_category_2 == "High Flow NC" ~ (0.48 + ((lpm_set - 6) * 0.02)),
    is.na(fio2_set_clean) & device_category_2 %in% c("Vent", "NIPPV", "CPAP") ~ fio2_lookup[as.character(lpm_set)],
    TRUE ~ NA_real_
  ))

## Select needed columns from resp_support
resp_support <- resp_support %>%
  select(hospitalization_id, device_category_2, recorded_dttm, fio2_combined, lpm_set) %>%
  mutate(meas_hour = hour(recorded_dttm),
         meas_date = as.Date(recorded_dttm))

## First, rank devices in order to select most intense device used for each hour
## Second, determine max FiO2 for each hour
## Third, associate most intense device with FiO2 for each hour

device_rank <- function(device_category_2) {
  case_when(
    device_category_2 == 'Vent' ~ 1,
    device_category_2 == 'NIPPV' ~ 2,
    device_category_2 == 'CPAP' ~ 3,
    device_category_2 == 'High Flow NC' ~ 4,
    device_category_2 == 'Face Mask' ~ 5,
    device_category_2 == 'Trach Collar' ~ 6,
    device_category_2 == 'Nasal Cannula' ~ 7,
    device_category_2 == 'Other' ~ 8,
    device_category_2 == 'Room Air' ~ 9,
    TRUE ~ NA_integer_
  )
}

resp_support <- resp_support %>%
  mutate(device_rank = device_rank(device_category_2))

### Max fio2 per hour
fio2_max <- resp_support %>%
  select(hospitalization_id, meas_date, meas_hour, fio2_combined) %>%
  filter(!is.na(fio2_combined)) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  slice_max(fio2_combined)

### Group by person, measurement date and measurement hour 
### Get most intense device within each hour
device_max <- resp_support %>%
  select(hospitalization_id, meas_date, meas_hour, device_rank) %>%
  filter(!is.na(device_rank)) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  slice_min(device_rank, with_ties = FALSE)

device_category <- function(device_rank) {
  case_when(
    device_rank == 1 ~ 'Vent',
    device_rank == 2 ~ 'NIPPV',
    device_rank == 3 ~ 'CPAP',
    device_rank == 4 ~ 'High Flow NC',
    device_rank == 5 ~ 'Face Mask',
    device_rank == 6 ~ 'Trach Collar',
    device_rank == 7 ~ 'Nasal Cannula',
    device_rank == 8 ~ 'Other',
    device_rank == 9 ~ 'Room Air',
    TRUE ~ NA_character_
  )
}

### Revert back to categories from rank
device_max <- device_max %>%
  mutate(device_category = device_category(device_rank))

### Add to encounters by hour
hosp_by_hour <- hosp_by_hour %>% 
  left_join(fio2_max, by = c("hospitalization_id", "meas_hour", "meas_date"))

hosp_by_hour <- hosp_by_hour %>% 
  left_join(device_max, by = c("hospitalization_id", "meas_hour", "meas_date"))

### Clear memory
rm(device_max)
rm(fio2_max)
rm(hospitalization)

## Carry forward device name until another device is recorded 
## or the end of the measurement time window
hosp_by_hour <- hosp_by_hour %>%
  group_by(hospitalization_id) %>%
  fill(device_category, .direction = "down") %>%
  mutate(device_filled = ifelse(is.na(device_category), NA, device_category))

## Carry forward FiO2 measurement name until another device is recorded 
## or the end of the measurement time window
hosp_by_hour <- hosp_by_hour %>%
  mutate(fio2_combined = as.double(fio2_combined)) %>%
  group_by(hospitalization_id, device_filled) %>%
  fill(fio2_combined, .direction = "down") %>%
  mutate(fio2_filled = ifelse(is.na(fio2_combined), NA, fio2_combined))

hosp_by_hour <- hosp_by_hour %>% 
  distinct() %>% 
  arrange(hospitalization_id, meas_date, meas_hour) %>% 
  select(hospitalization_id, meas_date, meas_hour, device_filled, fio2_filled)


cohort_table <- cohort_tracking("Adding Respiratory Support Data", hosp_by_hour, cohort_table)
cohort_table

# LABS
# For each lab, determine the worst in every hour
# Needed lab names
required_lab_names <- c("Platelet Count", "Bilirubin, Total", "Creatinine", "pO2", 
                        "Bilirubin, Indirect", "Arterial O2 pressure", "pCO2", 
                        "Creatinine, Whole Blood", "SvO2", "Oxygen Saturation", 
                        "Hemoglobin", "Bilirubin, Direct", "Arterial CO2 Pressure", 
                        "Arterial O2 Saturation")
## Load data
## Filter on required labs and then on specified parameters (removes outliers)
labs <- tbl(con, "clif_labs") %>%
  filter(lab_name %in% required_lab_names) %>% 
  filter(
    (lab_name == "Platelet Count" & lab_value_numeric > 0 & lab_value_numeric <= 832) | 
      (lab_name == "Bilirubin, Total" & lab_value_numeric > 0 & lab_value_numeric <= 50) |
      (lab_name == "Arterial O2 pressure" & lab_value_numeric > 30 & lab_value_numeric <= 800) |
      (lab_name == "Creatinine" & lab_value_numeric > 0 & lab_value_numeric <= 30)
  ) %>% 
  collect()

## Next, for each lab, determine the worst value in a particular hour
### PaO2: Minimum and then forward filling

pao2 <- labs %>%
  filter(lab_category == "po2_arterial") %>% 
  mutate(meas_hour = hour(lab_result_dttm),
         meas_date = as.Date(lab_result_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, lab_category, lab_value_numeric) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(min_pao2 = min(lab_value_numeric, na.rm = TRUE)) %>%
  ungroup()

ids_by_hour <- hosp_by_hour %>% 
  select(hospitalization_id, meas_date, meas_hour)

pao2_hours <- left_join(ids_by_hour, pao2) %>%
  arrange(hospitalization_id, meas_date, meas_hour) %>%
  distinct() %>% collect()

pao2_hours <- pao2_hours %>%
  mutate(dttm = as_datetime(paste0(meas_date, " ", meas_hour, ":00"), format = "%Y-%m-%d %H:%M"))

pao2_hours <- pao2_hours %>%
  mutate(last_measure = if_else(!is.na(min_pao2), dttm, NA)) %>%
  group_by(hospitalization_id) %>%
  fill(last_measure, .direction = "down") %>%
  ungroup()

# Calculate time difference between the hour we're trying to fill and the most recent PaO2, filter to only 3 additional hrs (4 total)
pao2_hours <- pao2_hours %>%
  mutate(hour_diff = as.numeric(difftime(dttm, last_measure, units = "hours"))) %>%
  filter(hour_diff >= 0 & hour_diff <= 3)

# Fill PaO2 forward
pao2_hours <- pao2_hours %>%
  group_by(hospitalization_id, last_measure) %>%
  fill(min_pao2, .direction = "down") %>%
  mutate(pao2_filled = if_else(is.na(min_pao2), NA_real_, min_pao2)) %>%
  ungroup()

pao2_filled <- pao2_hours %>%
  select(hospitalization_id, meas_date, meas_hour, pao2_filled)


### Bilirubin
bilirubin <- labs %>% 
  filter(lab_category == "bilirubin_total") %>% 
  mutate(meas_hour = hour(lab_result_dttm),
         meas_date = as.Date(lab_result_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, lab_category, lab_value_numeric) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(max_bilirubin = max(lab_value_numeric, na.rm = TRUE)) %>%
  ungroup()

### Platelets
plt_count <- labs %>% 
  filter(lab_category == "platelet_count") %>% 
  mutate(meas_hour = hour(lab_result_dttm),
         meas_date = as.Date(lab_result_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, lab_category, lab_value_numeric) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(min_plt_count = min(lab_value_numeric, na.rm = TRUE)) %>%
  ungroup()

### Creatinine
creatinine <- labs %>% 
  filter(lab_category == "creatinine") %>% 
  mutate(meas_hour = hour(lab_result_dttm),
         meas_date = as.Date(lab_result_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, lab_category, lab_value_numeric) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(max_creatinine = max(lab_value_numeric, na.rm = TRUE)) %>%
  ungroup()

## Add all lab data to main df

hosp_by_hour <- hosp_by_hour %>% 
  left_join(pao2_filled, by = c("hospitalization_id", "meas_hour", "meas_date")) %>% 
  left_join(creatinine, by = c("hospitalization_id", "meas_hour", "meas_date")) %>% 
  left_join(bilirubin, by = c("hospitalization_id", "meas_hour", "meas_date")) %>% 
  left_join(plt_count, by = c("hospitalization_id", "meas_hour", "meas_date"))

cohort_table <- cohort_tracking("Adding Lab Data", hosp_by_hour, cohort_table)
cohort_table

### Save some memory
rm(labs)
rm(bilirubin)
rm(creatinine)
rm(pao2_filled)
rm(pao2_hours)
rm(pao2)
rm(plt_count)
rm(resp_support)
rm(vital_min_max_time)


# BACK TO VITALS
# Need: SpO2 and MAP
required_vitals <- c("map", "spo2")

vitals <- tbl(con, "clif_vitals") %>%
  filter(vital_category %in% required_vitals) %>% 
  filter(
      (vital_category == "spo2" & vital_value > 60 & vital_value <= 100)) %>%
  inner_join(hosp_ids, by = "hospitalization_id", copy = TRUE) %>% 
  select(hospitalization_id, recorded_dttm, vital_category, vital_value) %>%
  collect()

# Get min SpO2 per hour
spo2 <- vitals %>%
  filter(vital_category == "spo2") %>% 
  mutate(meas_hour = hour(recorded_dttm),
         meas_date = as.Date(recorded_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, vital_category, vital_value) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(min_spo2 = min(vital_value, na.rm = TRUE)) %>%
  ungroup()

# Get min MAP per hour
map <- vitals %>%
  filter(vital_category == "map") %>% 
  mutate(meas_hour = hour(recorded_dttm),
         meas_date = as.Date(recorded_dttm)) %>%
  select(hospitalization_id, meas_date, meas_hour, vital_category, vital_value) %>%
  group_by(hospitalization_id, meas_date, meas_hour) %>%
  summarise(min_map = min(vital_value, na.rm = TRUE)) %>%
  ungroup()

## We can use SpO2 -> PaO2 conversion. This can help with PaO2 NAs.
## Source: Non-linear estimation from Brown et al in Critical Care Medicine August 2017 
## DOI: 10.1097/CCM.0000000000002514

calc_pao2 <- function(spo2) {
  spo2 <- spo2 / 100
  # Return NA for spo2 >= 1 or spo2 <= 0 to avoid divide by zero or log of negative
  invalid <- spo2 >= 1 | spo2 <= 0 | is.na(spo2)
  a <- 11700 / ((1 / spo2) - 1)
  b <- sqrt((50^3) + (a^2))
  pao2 <- ((b + a)^(1/3)) - ((b - a)^(1/3))
  pao2[invalid] <- NA_real_
  return(pao2)
}

# Add PaO2 calculation
spo2 <- spo2 %>%
  mutate(pao2_imputed = calc_pao2(min_spo2))

# Replace with NA if SpO2 >= 97
spo2 <- spo2 %>%
  mutate(pao2_imputed = ifelse(min_spo2>=97, NA, pao2_imputed))

# Join to main table
hosp_by_hour <- hosp_by_hour %>% 
  left_join(map, by = c("hospitalization_id", "meas_date", "meas_hour")) %>% 
  left_join(spo2, by = c("hospitalization_id", "meas_date", "meas_hour")) %>% 
  distinct()

cohort_table <- cohort_tracking("Adding SpO2 and PaO2", hosp_by_hour, cohort_table)
cohort_table

## Free up memory
rm(vitals)
rm(map)
rm(spo2)

# Create PaO2/FiO2 and SpO2/FiO2 ratio columns
hosp_by_hour <- hosp_by_hour %>%
  mutate(p_f = ifelse(!is.na(fio2_filled) & !is.na(pao2_filled), pao2_filled / fio2_filled, NA),
         p_f_imputed = ifelse(!is.na(fio2_filled) & !is.na(pao2_imputed), pao2_imputed / fio2_filled, NA),
         s_f = ifelse(!is.na(fio2_filled) & !is.na(min_spo2), min_spo2 / fio2_filled, NA)) %>%
  distinct()

# MEDS
## Filter to vasopressors
pressors <- tbl(con, "clif_medication_admin_continuous") %>%
  filter(med_group == "vasoactives") %>%
  mutate(meas_hour = hour(admin_dttm),
         meas_date = as.Date(admin_dttm)) %>%
  select(hospitalization_id, meas_hour, meas_date, med_category, med_dose) %>%
  distinct() %>%
  inner_join(hosp_ids, by = "hospitalization_id", copy = TRUE) %>% 
  collect()

## Get max num pressors (cap at 4) per hour
## Keep dobutamine alone
max_pressors <- pressors %>%
  group_by(hospitalization_id, meas_hour, meas_date) %>%
  summarise(
    med_list = unique(med_category),
    num_pressors = n_distinct(med_category),
    num_pressors = ifelse(num_pressors > 4, 4, num_pressors),
    dobutamine_alone = as.integer(all(med_list == "dobutamine") & length(med_list) == 1),
    .groups = "drop"
  )

## Pivot pressors wide by med_category and med_dose
## Select the maximum dose given in the hour
pressors <- pressors %>% 
  pivot_wider(names_from = med_category, 
              values_from = med_dose,
              values_fn = max) %>% 
  distinct()
## Merge max_pressors and pressors to main dataframe hosp_by_hour!
hosp_by_hour <- hosp_by_hour %>% 
  left_join(pressors, by = c("hospitalization_id", 
                             "meas_date", "meas_hour")) %>% 
  left_join(max_pressors, by = c("hospitalization_id", 
                                 "meas_date", "meas_hour")) %>% 
  distinct()

cohort_table <- cohort_tracking("Adding Meds", hosp_by_hour, cohort_table)
cohort_table

## Free up memory
rm(max_pressors)
rm(pressors)

# Patient Assessment
# Need: GCS
## Load data
assessments <- tbl(con, "clif_patient_assessments") %>% 
  filter(assessment_category == "gcs_total") %>%
  filter(numerical_value > 0) %>%
  mutate(meas_hour = hour(recorded_dttm),
         meas_date = as.Date(recorded_dttm)) %>% 
  select(hospitalization_id, meas_hour, meas_date, 
         assessment_category, numerical_value) %>% 
  distinct() %>% 
  inner_join(hosp_ids, by = "hospitalization_id", copy = TRUE) %>% 
  collect()

assessments <- assessments %>% 
  pivot_wider(names_from = assessment_category, 
              values_from = numerical_value,
              values_fn = min)

## Merge with main dataframe hosp_by_hour!
hosp_by_hour <- hosp_by_hour %>% 
  left_join(assessments, by = c("hospitalization_id", 
                                "meas_date", "meas_hour"))

cohort_table <- cohort_tracking("Adding GCS", hosp_by_hour, cohort_table)
cohort_table

# LIFE SUPPORT
## Flag if on life support in a given hour
hosp_by_hour <- hosp_by_hour %>%
  mutate(on_life_support = case_when(
    num_pressors >= 1 ~ 1,
    device_filled %in% c('NIPPV', 'Vent', 'High Flow NC') ~ 1,
    p_f < 200 ~ 1,
    p_f_imputed < 200 ~ 1,
    TRUE ~ 0
  ))

## Create leading flags to identify first episode of 6 consecutive hours of life support
hosp_by_hour <- hosp_by_hour %>%
  arrange(hospitalization_id, meas_date, meas_hour) %>%
  group_by(hospitalization_id) %>%
  mutate(
    lead_1 = lead(on_life_support, 1),
    lead_2 = lead(on_life_support, 2),
    lead_3 = lead(on_life_support, 3),
    lead_4 = lead(on_life_support, 4),
    lead_5 = lead(on_life_support, 5)) %>%
  mutate(
    across(c(on_life_support, lead_1, lead_2, lead_3, lead_4, lead_5), ~ replace_na(.x, 0))) %>%
  mutate(
    lead_values = rowSums(across(c(on_life_support, lead_1, lead_2, lead_3, lead_4, lead_5))),
    life_support_sum = ifelse(lead_values == 6, 6, 0)) %>%
  ungroup()

## Get time of first episode of 6 consecutive hours of life support started
ls_encs <- hosp_by_hour %>%
  filter(life_support_sum == 6) %>%
  mutate(life_support_start = as.POSIXct(paste0(meas_date, " ", meas_hour, ":00"), 
                                         format = "%Y-%m-%d %H:%M", tz="UTC")) %>%
  group_by(hospitalization_id) %>%
  summarise(life_support_start = min(life_support_start, na.rm=T)) %>%
  ungroup() %>%
  mutate(window_start = as.POSIXct((life_support_start - hours(42)), tz="UTC"),
         window_end = as.POSIXct((life_support_start + hours(5)), tz="UTC"))

## Explode between window start and window end to get all hourly timestamps
ls_encs_hours <- ls_encs %>%
  rowwise() %>%
  mutate(txnDt = list(seq.POSIXt(window_start, window_end, by = "hour"))) %>%
  unnest(txnDt) %>%
  mutate(meas_hour = hour(txnDt),
         meas_date = as.Date(txnDt)) %>% 
  arrange(hospitalization_id, txnDt) 

## The window that we care about is 42 hours pre and 6 hours post life support start
## where patient is on life support for at least 6 hours after starting

hosp_by_hour_ls <- ls_encs_hours %>% 
  left_join(hosp_by_hour, by = c("hospitalization_id", "meas_hour", "meas_date"))

cohort_table <- cohort_tracking("Life Support Only", hosp_by_hour_ls, cohort_table)
cohort_table

# Bring in patient demographics
hosp_patient_ids <- tbl(con, "clif_hospitalization") %>% 
  select(hospitalization_id, patient_id, age_at_admission,
         zipcode_nine_digit, zipcode_five_digit, census_block_group_code, 
         admission_dttm, discharge_dttm) %>% 
  distinct() %>% 
  collect()

patient <-  tbl(con, "clif_patient") %>%
  select(patient_id, race_category, ethnicity_category, sex_category, death_dttm) %>% 
  distinct() %>% 
  collect()  

# Determine in-hospital mortality
hosp_with_mortality <- hosp_patient_ids %>% 
  left_join(patient, by = "patient_id") %>% 
  mutate(in_hospital_mortality = if_else(
    !is.na(death_dttm) & death_dttm >= admission_dttm & death_dttm <= discharge_dttm,
    1, 0
  )) %>% 
  distinct()

# Finalize cohort and export
cohort <- hosp_by_hour_ls %>% 
  left_join(hosp_with_mortality, by = "hospitalization_id")

cohort_final <- cohort %>% 
  select(-lead_1, -lead_2, -lead_3, -lead_4, -lead_5, 
         -life_support_sum, -txnDt, -med_list, 
         -lead_values, -pao2_filled, -p_f) %>%
  distinct()

cohort_table <- cohort_tracking("Final", cohort_final, cohort_table)
cohort_table

write_parquet(cohort_final, paste0(output_path, "/sofaclif_cohort", ".parquet"))

# Clear
rm(list = ls())


