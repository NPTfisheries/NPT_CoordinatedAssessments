# ------------------------
# Author(s): Mike Ackerman
# Purpose: Prepare LGR escapement and spawner abundance results from Bill Young's fall Chinook run reconstruction work to push to 
#   Coordinated Assessments natural-origin spawner abundance (NOSA) table.
# 
# Created Date: May 6, 2026
#   Last Modified: 
#
# Notes: 

# clear environment
rm(list = ls())

# load libraries
library(readxl)
library(tidyverse)
library(writexl)

#----------------------------------
# retrieve population and NOSA info from CAX

# remotes:::install_github("nwfsc-cb/rCAX@*release")
library(rCAX)

# retrieve key, if needed
#cax_key = Sys.getenv("CAX_KEY")

# retrieve populations table from CAX
fchnk_pop_df = rcax_table_query(tablename = "Populations") %>%
  filter(esudps == "Salmon, Chinook (Snake River fall-run ESU)") %>%
  select(
    CommonName      = commonname,
    CommonPopName   = trt_pop_id,
    Run             = run,
    RecoveryDomain  = recoverydomain,
    ESUDPS         = esudps,
    MajorPopGroup   = majorpopgroup,
    PopID           = id,           
  )

# retrieve NPT tables from CAX; need to see which TimeSeriesIDs have been used by NPT
npt_cax = rcax_hli("NOSA", qlist = list(limit = 10000)) %>%
  transmute(
    submitagency = submitagency,
    compilerrecordid = compilerrecordid,
    tbl = "NOSA"
  ) %>%
  bind_rows(rcax_hli("JuvOut", qlist = list(limit = 10000)) %>%
              transmute(submitagency = submitagency,
                        compilerrecordid = compilerrecordid,
                        tbl = "JuvOut")
            ) %>%
  filter(submitagency == "NPT",
         compilerrecordid != "") %>%
  mutate(timeseriesid = stringr::str_sub(compilerrecordid, 1, 5))

# get the next two available time series IDs
avail_ts_ids        = setdiff(22500:24999, as.integer(npt_cax$timeseriesid))[1:2] 
names(avail_ts_ids) = c("Escapement", "NOSA")

#-------------------------------------------------------------
# read in time-stamped fall chinook run reconstruction results
fchnk_lgr_df = read_xlsx(path = "data/Fall Chinook Run Rec/escp to & abv LGR incl AD & NO totals disp fidelity 20260506.xlsx",
                         sheet = "Esc Abv",
                         range = "A8:AN59") %>%
  select(
    run_year                    = year,
    # escapement to LGR
    esc_2_lgr_tot_adults        = `Total adults`,
    esc_2_lgr_tot_jacks         = `Total jacks`,
    esc_2_lgr_tot               = `total SR fchnk`,
    esc_2_lgr_nat_adults        = `Natural adult`,
    esc_2_lgr_hat_adults        = `Hatchery adults`,
    esc_2_lgr_nat_jacks         = `Natural jack`,
    esc_2_lgr_hat_jacks         = `hatchery jack`,
    # escapement above LGR, adjusted for broodstock removals and fallback
    esc_abv_lgr_hat_adults      = `Hat adults`,
    esc_abv_lgr_hat_jacks       = `Hat jacks`,
    esc_abv_lgr_nat_adults      = `Nat adults`,
    esc_abv_lgr_nat_jacks       = `Nat Jacks`,
    # returns prior to August 18
    b4_818_hat_adults           = `Adult Hat`,
    b4_818_nat_adults           = `Adult Nat`,
    # total escapement above LGR, adjusted for returns prior to Aug 18
    tot_esc_abv_lgr_hat_adults  = `HAT adults...15`,
    tot_esc_abv_lgr_nat_adult   = `NAT adults...16`,
    tot_esc_abv_lgr_tot_adults  = `All adults`,
    tot_esc_abv_lgr_phos_adults = `HOR to LGR adult`,
    tot_esc_abv_lgr_phos_all    = `HOR to LGR all`,
    esc_data_sourc              = `Escapement data source`,
    # final spawner abundance above LGR, adjusted for harvest above LGR & volunteers to NPTH
    fin_abv_lgr_hosa_adults     = `HAT adults...33`,
    fin_abv_lgr_hosa_jacks      = `HAT jacks`,
    fin_abv_lgr_nosa_adults     = `NAT adults...35`,
    fin_abv_lgr_nosa_jacks      = `NAT jacks`,
    fin_abv_lgr_tot_adults      = `Total Adults`,
    fin_abv_lgr_tot_all         = `Total all fish (adlt+jack)`,
    fin_abv_lgr_phos_adults     = `pHOS adult`,
    fin_abv_lgr_phos_all        = `pHOS all`
  ) 

#--------------------------
# prep fchnk_lgr_df for CAX
fchnk_prep_df = fchnk_lgr_df %>%
  mutate(
    esc_2_lgr_nat_all     = rowSums(across(c(esc_2_lgr_nat_adults, esc_2_lgr_nat_jacks)), na.rm = TRUE),
    esc_2_lgr_phos_all    = rowSums(across(c(esc_2_lgr_hat_adults, esc_2_lgr_hat_jacks)), na.rm = TRUE) / esc_2_lgr_tot,
    esc_2_lgr_phos_adults = esc_2_lgr_hat_adults / esc_2_lgr_tot_adults,
    #esc_abv_lgr_nat_all   = rowSums(across(c(esc_abv_lgr_nat_adults, esc_abv_lgr_nat_jacks)), na.rm = TRUE),
    fin_abv_lgr_nosa      = rowSums(across(c(fin_abv_lgr_nosa_adults, fin_abv_lgr_nosa_jacks)), na.rm = TRUE)
  ) %>%
  mutate(
    run_year = as.integer(run_year),
    across(
      .cols = where(is.double) & !contains("phos"),
      .fns  = ~ round(.x, 0)
    ),
    across(
      .cols = where(is.double) & contains("phos"),
      .fns  = ~ round(.x, 3)
    )
  ) %>%
  # build EstimateType Escapement vs. NOSA
  transmute(
    SpawningYear = run_year,
    # Escapement estimates
    Escapement_NOSAIJ = esc_2_lgr_nat_all,
    Escapement_NOSAEJ = esc_2_lgr_nat_adults,
    Escapement_pHOSij = esc_2_lgr_phos_all,
    Escapement_pHOSej = esc_2_lgr_phos_adults,
    # NOSA estimates
    NOSA_NOSAIJ = fin_abv_lgr_nosa,
    NOSA_NOSAEJ = fin_abv_lgr_nosa_adults,
    NOSA_pHOSij = fin_abv_lgr_phos_all,
    NOSA_pHOSej = fin_abv_lgr_phos_adults
  ) %>%
  pivot_longer(
    cols      = -SpawningYear,
    names_to  = c("EstimateType", ".value"),
    names_sep = "_"
  ) %>%
  mutate(
    CommonPopName = "SNMAI",
    # location info
    WaterBody      = case_when(
      EstimateType == "Escapement" ~ "Snake River",
      EstimateType == "NOSA"       ~ "Multiple"
    ),
    EscapementLong = if_else(EstimateType == "Escapement", -117.433225, NA_real_),  # lat/lon from PTAGIS for GRA
    EscapementLat  = if_else(EstimateType == "Escapement", 46.657760,   NA_real_),
    PopFit         = "Portion",
    PopFitNotes    = "Estimate reflects fish returning to or above Lower Granite Dam and therefore represents only a portion of the total population.",
    # add TimeSeriesID and CompilerRecordID
    TimeSeriesID     = unname(avail_ts_ids[EstimateType]),
    CompilerRecordID = paste0(TimeSeriesID, "-", SpawningYear),
    # estimate info
    EscapementTiming      = if_else(EstimateType == "Escapement", "Aug-Dec", NA_character_),
    MethodNumber          = 1, # just needs to be unique to population and year
    BestValue             = "Yes",
    ProtMethName          = case_when(
      SpawningYear %in% 1975:1990               ~ "Unknown run reconstruction.",
      SpawningYear ==   1991                    ~ "Cooney, T. (1991).",
      SpawningYear %in% c(1992:1994, 1996:1999) ~ "Lavoy, L. WDFW, Stock composition report, Columbia River Laboratory.",
      SpawningYear ==   1995                    ~ "Lavoy, L. and G. Mendel (1996).",
      SpawningYear %in% 2000:2002               ~ "Sand, N.J. WDFW, run reconstruction.",
      SpawningYear %in% 2003:2019               ~ "Young et al. (2022).",
      SpawningYear %in% 2020:2025               ~ "Young et al. (2023)."
    ),
    ProtMethDocumentation = case_when(
      SpawningYear %in% 1975:1990               ~ "T. Cooney, pers. comm.",
      SpawningYear ==   1991                    ~ "Cooney, T. 1991. Estimation of Snake River fall chinook returns to Ice Harbor, LF Hatchery, and over Lower Granite. Washington Department of Fisheries memorandum, May 7, 1991.",
      SpawningYear %in% c(1992:1994, 1996:1999) ~ "LaVoy, L.W. 1993. Stock composition of fall Chinook at Lower Granite Dam in 1992. Washington Department of Fish and Wildlife, Columbia River Laboratory Report.",
      SpawningYear ==   1995                    ~ "LaVoy, L.W., and G. Mendel. 1996. Stock composition of fall Chinook at Lower Granite Dam in 1995. Washington Department of Fish and Wildlife, Columbia River Laboratory Report 96-13, Battle Ground.",
      SpawningYear %in% 2000:2002               ~ "Sand, N.J. 2003. WDFW Annual Report.",
      SpawningYear %in% 2003:2019               ~ "Young, W.P., S. Rosenberger, and D. Milks. 2022. Snake River Fall Chinook Salmon Run Reconstruction at Lower Granite Dam; Methods for Retrospective Analysis. Nez Perce Tribe, Department of Fisheries Resources Management.",
      SpawningYear %in% 2020:2025               ~ "Young, W., S. Rosenberger, J. Bumgarner, J. Fortier, B. Sandford, and A. Harris. 2023. Snake River fall Chinook salmon Lower Granite Dam run reconstruction report; return year 2022.",
    ),
    MethodAdjustments     = case_when(
      EstimateType == "Escapement" ~ "Estimate of escapement to LGR, unadjusted for fallback, broodstock removals, and harvest above LGR.",
      EstimateType == "NOSA"       ~ "Estimate of escapement past LGR adjusted for fallback, broodstock removals, and harvest above LGR."
    ),
    NullRecord            = "No",
    DataStatus            = "Reviewed",
    MetaComments          = "Snake River Fall Chinook Run Reconstruction",
    HLI                   = "NOSA",
    # data info
    OtherDataSources  = "IDFG | NOAA | Fish Passage Center | Idaho Power",
    IndicatorLocation = "npt-cdms.nezperce.org",
    MetricLocation    = "npt-cdms.nezperce.org",
    MeasureLocation   = "npt-cdms.nezperce.org",
    # contact and submittal info
    ContactAgency      = "Nez Perce Tribe",
    ContactAgy         = "NPT",
    ContactPersonFirst = "Bill",
    ContactPersonLast  = "Young",
    ContactPhone       = "208-621-4909",
    ContactEmail       = "billy@nezperce.org",
    SubmitAgency       = "NPT",
    DataEntry          = "Mike Ackerman",
    UpdDate            = "2026/05/06 07:10:00", # the latest timestamp on fall chinook run reconstruction results stored in NPT_CAX
    Publish            = "Yes",
    DataEntryNote      = "Information compiled from Snake River fall Chinook run reconstruction outputs and associated code maintained in the NPT_CAX GitHub repository."
  ) %>%
  left_join(fchnk_pop_df, by = "CommonPopName")

#-------------------------------------------------------------
# reorder and QC columns to follow CAX data exchange standards
source("R/nosa_des_spec.R")

# re-order & add missing columns
fchnk_to_cax = apply_cax_des_col_order(fchnk_prep_df, nosa_des_spec)

# QC column types
qc_report = qc_against_des_spec(fchnk_to_cax, nosa_des_spec)

# write to excel
write_xlsx(fchnk_to_cax, path = paste0("output/Fall_Chinook_4_CAX_NOSA_", Sys.Date(), ".xlsx"))

#---------------------------------------------------------------------------------------------------------------------------
# replace NOSA table in most recent Access DB (for unknown reasons, I needed to use RODBC instead of DBI to write the table)
library(RODBC)

# re-connect to access db
channel = odbcConnectAccess2007("data/20260317 NPT StreamNet API interface DES version 2024.1.accdb")

# remove the existing NOSA table
sqlDrop(channel, "NOSA", errors = FALSE)

# write srfs_to_cax to db
sqlSave(channel, fchnk_to_cax, tablename = "NOSA", rownames = FALSE)

# disconnect from access db
close(channel)

# NOTE: After pushing to access database, make the following changes to data formats:
# ID: Indexed = Yes (No Duplicates)
# CompilerRecordID: Required = Yes, Indexed = Yes (No Duplicates)

### END SCRIPT
