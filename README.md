---
editor_options: 
  markdown: 
    wrap: 72
---

# *CLIF Project Title*

## CLIF VERSION

2.1

## Objective

*The purpose of this project is to develop a more robust, statistically
sound model for in-hospital mortality using the variables outlined by
the Sequential Organ Failure Assessment (SOFA) score.*

## Required CLIF tables and fields

Please refer to the online [CLIF data
dictionary](https://clif-consortium.github.io/website/data-dictionary.html),
[ETL
tools](https://github.com/clif-consortium/CLIF/tree/main/etl-to-clif-resources),
and [specific table
contacts](https://github.com/clif-consortium/CLIF?tab=readme-ov-file#relational-clif)
for more information on constructing the required tables and fields.

1\. **patient**: `patient_id`, `race_category`, `ethnicity_category`,
`sex_category`

2\. **hospitalization**: `patient_id`, `hospitalization_id`,
`admission_dttm`, `discharge_dttm`, `age_at_admission`, `death_dttm`

3\. **vitals**: `hospitalization_id`, `recorded_dttm`, `vital_value` -
`vital_category == 'map'|'spo2'|'weight_kg'`

4\. **labs**: `hospitalization_id`, `lab_result_dttm`, `lab_category`,
`lab_value` -
`lab_category == 'bilirubin_total'|'creatinine'|'platelet_count'|'pao2_arterial'`

5\. **medication_admin_continuous**: `hospitalization_id`, `admin_dttm`,
`med_name`, `med_category`, `med_dose`, `med_dose_unit` -
`med_group == 'vasoactives'`

6\. **respiratory_support**: `hospitalization_id`, `recorded_dttm`,
`device_category`, `mode_category`, `tracheostomy`, `fio2_set`,
`lpm_set`, `resp_rate_set`, `peep_set`, `resp_rate_obs`

## Cohort identification

*Describe study cohort inclusion and exclusion criteria here*

## Expected Results

*Describe the output of the analysis. The final project results should
be saved in the [`output/final`](output/README.md) directory.*

## Detailed Instructions for running the project

## 1. Update `config/config.json`

Follow instructions in the [config/README.md](config/README.md) file for
detailed configuration steps.

## 2. Set up the project environment

Run `00_renv_restore.R` in the [code](code/templates/R) to set up the
project environment

## 3. Run code

Detailed instructions on the code workflow are provided in the [code
directory](code/README.md)
