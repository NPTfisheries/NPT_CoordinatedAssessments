# ------------------------
# Author(s): Mike Ackerman
# Purpose: Prepare latest SnakeRiverFishStatus natural-origin spawner abundance (NOSA) estimates for upload to Coordinated Assessments (CAX) database. 
#   The goal is to replace all past NOSA estimates uploaded by NPT with the latest and provide them in a standardized manner that makes clear
#   IPTDS-based escapement estimates versus expanded spawner abundance estimates (accounting for unmonitored habitat). 
# 
# Created Date: January 23, 2026
#   Last Modified: March 10, 2026
#
# Notes:

# clear environment
rm(list = ls())

# load libraries
library(tidyverse)
library(readxl)
library(PITcleanr)
library(sf)
library(writexl)

#-------------------------------------
# read in SnakeRiverFishStatus results
srfs_results_files = list.files("data/SRFS",
                                pattern = "(Chinook|Steelhead).*\\.xlsx$",
                                full.names = TRUE) %>%
  discard(~ grepl("~\\$", basename(.x)))
  
# population & site estimates, age proportions
pop_esc_df  = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Pop_Tot_Esc"))
site_esc_df = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Site_Esc"))
age_p_df    = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Pop_Age_Props"))

#----------------------------------------------------------------------------------
# retrieve NOSA tbl from StreamNet API interface Access DB - need TimeSeriesID info
source("R/connectNPTCAdbase.R")
con = connectNPTCAdbase("data/StreamNet API interface DES version 2024.1 - NPT.accdb")
accdb_nosa_tbl = DBI::dbReadTable(con, "NOSA")
DBI::dbDisconnect(con)

# summarize TimeSeriesID info
ts_tbl = accdb_nosa_tbl %>%
  select(CommonName, CommonPopName, SpawningYear, TimeSeriesID) %>%
  distinct(CommonName, CommonPopName, TimeSeriesID) %>%
  arrange(CommonName, CommonPopName) %>%
  mutate(CommonName = recode(CommonName, "Chinook salmon" = "Chinook Salmon"))

#----------------------------------------
# retrieve locations for PTAGIS INT sites
site_ll = queryInterrogationMeta() %>%
  select(site_code = siteCode, latitude, longitude) %>%
  bind_rows(queryMRRMeta() %>% select(site_code = siteCode, latitude, longitude)) %>%
  filter(!is.na(latitude) & !is.na(longitude)) %>%
  transmute(site_code,
            # data exchange standards specify 4 decimals
            EscapementLong = round(longitude, 4),
            EscapementLat  = round(latitude, 4))

#------------------------
# retrieve data from CAX

# install rCAX, if needed
# remotes:::install_github("nwfsc-cb/rCAX@*release")
library(rCAX)

# retrieve key, if needed
#cax_key = Sys.getenv("CAX_KEY")

# retrieve metadata for NOSA fields
cax_nosa_meta = rcax_hli("NOSA", type = "colnames")

# retrieve NOSA table, up to 10,000 records (by default, only retrieves 1,000)
cax_nosa = rcax_hli("NOSA", qlist = list(limit = 10000))

#-----------------------------------------------------------------------------------
# trim to records submitted by NPT and flag older submittals (by group) for deletion
npt_cax_nosa = cax_nosa %>%
  filter(submitagency == "NPT")

npt_cax_nosa_flagged = npt_cax_nosa %>%
  transmute(
    CommonName    = commonname,
    CommonPopName = commonpopname,
    SpawningYear  = spawningyear,
    MetaComments  = metacomments,
    ID            = id,
    lastupdated   = as.Date(lastupdated, format = "%Y/%m/%d")
  ) %>%
  filter(MetaComments == "STADEM and DABOM") %>%
  group_by(CommonName, CommonPopName, SpawningYear, MetaComments) %>%
  arrange(lastupdated, .by_group = TRUE) %>%
  mutate(
    n_records = n(),
    delete_old = n_records > 1 & row_number() < n()
  ) %>%
  ungroup()

# datasets available in CAX
cax_datasets = rcax_datasets()

# load populations table from CAX
pop_df = rcax_table_query(tablename = "Populations")

# load super populations table from CAX
#superpop_df = rcax_table_query(tablename = "SuperPopulations")

# clean up pop_df
sr_pop_df = pop_df %>%
  filter(str_detect(esudps, "Snake River"),
         str_detect(commonname, "Chinook|Steelhead"),
         !is.na(trt_pop_id),
         trt_pop_id != "",
         !popstatus == "Extirpated") %>%
  select(
    CommonName      = commonname,
    CommonPopName   = trt_pop_id,
    Run             = run,
    RecoveryDomain  = recoverydomain,
    ESU_DPS         = esudps,
    MajorPopGroup   = majorpopgroup,
    PopID           = id,           
    #NMFS_POPID      = nmfs_popid,
    #LocationName    = locationname,   
    #PopulationName  = populationname,
    #ESApopName      = esapopname,
    #NMFS_Population = nmfs_population
  ) %>%
  arrange(CommonName, MajorPopGroup, CommonPopName)

#-----------------------------------------------------------------
# prep age_p_df to join to abundance results and formatted for CAX
age_p_for_cax = age_p_df %>%
  select(species:upper95ci) %>%
  mutate(across(c(median, lower95ci, upper95ci), ~ round(.x, 8)),
         param = str_remove(param, "^p_"),
         param = str_replace(param, "age_", "Age")) %>%
  #group_by(species, spawn_yr, popid) %>%
  pivot_longer(
    cols = c(median, lower95ci, upper95ci),
    names_to = "stat",
    values_to = "value"
  ) %>%
  mutate(
    stat = recode(stat,
                  median    = "Prop",
                  lower95ci = "PropLowerLimit",
                  upper95ci = "PropUpperLimit")
  ) %>%
  pivot_wider(
    names_from = c(param, stat),
    values_from = value,
    names_glue = "{param}{stat}"
  )

#----------------------------------------------------------------
# prep SnakeRiverFishStatus results to compare and upload to NOSA
source("R/waterbody_lookup.R")

# the threshhold on which to consider a pop fully monitored by IPTDS, what do we want to set this at? 
threshhold = 0.95

# initial prep of SnakeRiverFishStatus results
srfs_base = pop_esc_df %>%
  # join prepped age proportions
  left_join(age_p_for_cax, by = c("species", "spawn_yr", "popid")) %>%
  # rename some columns to match CAX
  rename(CommonName    = species,
         SpawningYear  = spawn_yr,
         CommonPopName = popid,
         Comments      = notes) %>%
  # trim unused columns
  select(-mpg, -incl_sites, -n_tags, -mean, -mode, -sd, -cv, -p_qrf_se) %>%
  # CRSFC-s & SCUMA: use estimates from SC1 as it provides the longer time-series; SC3 didn't operated until spawn year 2022
  filter(!CommonPopName %in% c("CRSFC-s", "SCUMA")) %>%
  mutate(
    CommonPopName = case_when(
      CommonPopName == "CRLMA-s/CRSFC-s" ~ "CRSFC-s",
      CommonPopName == "SCLAW/SCUMA"     ~ "SCUMA",
      TRUE                               ~ CommonPopName
    ),
    # for CRSFC-s & SCUMA, set p_qrf to 1 and use un-expanded estimates
    p_qrf         = if_else(CommonPopName %in% c("CRSFC-s", "SCUMA"), 1, p_qrf),
    median_exp    = if_else(CommonPopName %in% c("CRSFC-s", "SCUMA"), median,    median_exp),
    lower95ci_exp = if_else(CommonPopName %in% c("CRSFC-s", "SCUMA"), lower95ci, lower95ci_exp),
    upper95ci_exp = if_else(CommonPopName %in% c("CRSFC-s", "SCUMA"), upper95ci, upper95ci_exp)
  ) %>%
  # toss out Tucannon estimates & estimates for multiple populations
  filter(!str_detect(CommonPopName, "/"),  # these could later be submitted as SuperPopulations
         !str_detect(CommonPopName, "SNTUC"))

# prep/reconcile SnakeRiverFishStatus results for CAX NOSA table
srfs_prep_4_cax = srfs_base %>%
  # attach population metadata from sr_pop_df
  mutate(CommonName = recode(CommonName, "Chinook" = "Chinook Salmon")) %>%
  left_join(sr_pop_df, by = c("CommonName", "CommonPopName")) %>%
  # add lat/lon based on the first site in pop_sites
  mutate(site_code = str_extract(pop_sites, "^[^,]+")) %>%
  left_join(site_ll, by = "site_code") %>%
  select(-site_code) %>%
  # set PopFit for median, lower95ci, and upper95ci based on proportion of habitat monitored; expanded ests will be added below
  mutate(PopFit = if_else(p_qrf >= threshhold, "Same", "Portion"),
         MetaComments = "STADEM and DABOM") %>%
  {
    df = .
    bind_rows(
      df,
      df %>%
        filter(
          p_qrf < threshhold,
          PopFit == "Portion",
          !is.na(median_exp),
          !is.na(lower95ci_exp),
          !is.na(upper95ci_exp)
        ) %>%
        mutate(
          median       = median_exp,
          lower95ci    = lower95ci_exp,
          upper95ci    = upper95ci_exp,
          PopFit       = "Same",
          MetaComments = "STADEM, DABOM, and QRF"
        )
    )
  } %>%
  # expanded ests no longer needed
  select(-contains("_exp")) %>%
  # assign MethodNumber
  mutate(MethodNumber = case_when(
    MetaComments == "STADEM and DABOM"       ~ 2,
    MetaComments == "STADEM, DABOM, and QRF" ~ 3,
    TRUE ~ NA_integer_
  )) %>%
  # round estimates and rename, add alpha
  mutate(across(c(median, lower95ci, upper95ci), round)) %>%
  rename(NOSAIJ           = median,
         NOSAIJLowerLimit = lower95ci,
         NOSAIJUpperLimit = upper95ci) %>%
  mutate(NOSAIJAlpha      = 0.05) %>%
  # mark all estimates as "Escapement" i.e., spawning mortality & harvest not accounted for
  mutate(EstimateType = "Escapement") %>%
  # assign EscapementTiming by species
  mutate(EscapementTiming = case_when(
    CommonName == "Chinook Salmon" ~ "Jun-Oct",
    CommonName == "Steelhead"      ~ "Feb-Jun",
    TRUE                           ~ NA_character_
  )) %>%
  # add PopFitNotes
  mutate(
    site_note = paste0("Estimate reflects PTAGIS site(s): ", pop_sites, ", monitoring ", round(p_qrf * 100, 1), "% of available habitat."),
    qrf_note = "Habitat coverage estimated using the redd QRF dataset (See et al. 2021).",
    exp_note = paste0("Estimate reflects full population (100% available habitat) by expanding the 'Portion' estimate by ", round(1 / p_qrf, 2),
      ". The 'Portion' estimate reflects PTAGIS site(s): ", pop_sites, " monitoring ", round(p_qrf * 100, 1), "% of available habitat."),
    PopFitNotes = case_when(
      MetaComments == "STADEM and DABOM" & PopFit == "Same"       ~ paste0(site_note, " PopFit = 'Same' (>= ", threshhold * 100, "%). ", qrf_note),
      MetaComments == "STADEM and DABOM" & PopFit == "Portion"    ~ paste0(site_note, " PopFit = 'Portion' (< ", threshhold * 100, "%). ", qrf_note),
      MetaComments == "STADEM, DABOM, and QRF" & PopFit == "Same" ~ exp_note,
      TRUE ~ NA_character_
    )
  ) %>%
  select(-site_note, -qrf_note, -exp_note) %>%
  # as default, set BestValue to be "Yes" for "Same" estimates
  mutate(BestValue = if_else(PopFit == "Same", "Yes", "No")) %>%
  # add ID for records to update or delete (i.e., already in CAX)
  full_join(npt_cax_nosa_flagged %>%
              select(CommonName,
                     CommonPopName,
                     SpawningYear,
                     MetaComments,
                     ID,
                     delete_old),
            by = c("CommonName", "CommonPopName", "SpawningYear", "MetaComments"),
            relationship = "many-to-many") %>%
  group_by(CommonName, CommonPopName, SpawningYear, MetaComments) %>%
  mutate(
    StatusMA = case_when(
      delete_old     & !is.na(ID) ~ "DELETE",
      is.na(NOSAIJ)  & !is.na(ID) ~ "DELETE",
      !is.na(NOSAIJ) & !is.na(ID) ~ "UPDATE",
      !is.na(NOSAIJ) & is.na(ID)  ~ "NEW"
    )
  ) %>%
  ungroup() %>%
  select(StatusMA, everything(), -delete_old) %>%
  # add protocol fields
  mutate(
    ProtMethName = case_when(
      MetaComments == "STADEM and DABOM"       ~ "PIT tag Based Escapement Estimation Above Lower Granite Dam v1.0",
      MetaComments == "STADEM, DABOM, and QRF" ~ "Ackerman et al. (In Prep)",
      TRUE ~ NA_character_
    ),
    ProtMethURL = case_when(
      MetaComments == "STADEM and DABOM"       ~ "https://www.monitoringresources.org/Document/Protocol/Details/2187",
      MetaComments == "STADEM, DABOM, and QRF" ~ "https://github.com/NPTfisheries/SnakeRiverPopAbundPaper",
      TRUE ~ NA_character_
    ),
    ProtMethDocumentation = paste0("See, K.E., R.N. Kinzer, and M.W. Ackerman. 2021. State-Space Model to Estimate Salmon Escapement Using Multiple Data Sources. North American Journal of Fisheries Management; ",
                                   "Waterhouse, L., J. White, K. See, A. Murdoch, and B.X. Semmens. 2020. A Bayesian Nested Patch Occupancy Model to Estimate Steelhead Movement and Abundance. Ecological Applications; ",
                                   "Kinzer, R., R. Orme, M. Campbell, J. Hargrove, and K. See. 2020. Report to NOAA Fisheries for 5-year ESA Status Review: Snake River Basin Steelhead and Chinook Salmon Population Abundance, Life History, and Diversity Metrics Calculated from In-Stream PIT-Tag Observations (SY2010-SY2019). In-stream PIT-tag Detection Systems Workgroup. 118 pp.; ",
                                   "Ackerman et al. (In Prep).")
  ) %>%
  # assign WaterBody based on pop_sites (unexpanded) or CommonPopName (expanded)
  left_join(waterbody_sites_lu, by = "pop_sites") %>%
  left_join(waterbody_pop_lu, by = "CommonPopName") %>%
  mutate(
    WaterBody = case_when(
      MetaComments == "STADEM and DABOM"       ~ WaterBody_site,
      MetaComments == "STADEM, DABOM, and QRF" ~ WaterBody_pop,
      TRUE                                     ~ NA_character_
    )
  ) %>%
  select(-WaterBody_site, -WaterBody_pop) %>%
  # additional metadata
  mutate(ContactPersonFirst = "Mike",
         ContactPersonLast  = "Ackerman",
         ContactPhone       = "208-634-5290",
         ContactEmail       = "mikea@nezperce.org",
         ContactAgency      = "Nez Perce Tribe",
         ContactAgy         = "NPT",
         SubmitAgency       = "NPT",
         HLI                = "NOSA",
         NullRecord         = "No",
         DataStatus         = "Reviewed",
         IndicatorLocation  = "npt-cdms.nezperce.org",
         MetricLocation     = "npt-cdms.nezperce.org",
         MeasureLocation    = "npt-cdms.nezperce.org",
         OtherDataSources   = "IDFG, ODFW, WDFW, Biomark, QCI, SBT, CTUIR",
         Publish            = "Yes") %>%
  # apply TimeSeriesIDs: apply existing ones for unexpanded ests, apply new ones for expanded ests
  left_join(
    ts_tbl,
    by = c("CommonName", "CommonPopName")
  ) %>%
  {
    df <- .
    
    new_ts_ids <- df %>%
      filter(MetaComments == "STADEM, DABOM, and QRF" | is.na(TimeSeriesID)) %>%
      distinct(CommonName, CommonPopName, MetaComments) %>%
      arrange(CommonName, CommonPopName, MetaComments) %>%
      mutate(TimeSeriesID_new = max(ts_tbl$TimeSeriesID, na.rm = TRUE) + row_number())
    
    stopifnot(max(new_ts_ids$TimeSeriesID_new, na.rm = TRUE) <= 24999)
    
    df %>%
      left_join(
        new_ts_ids,
        by = c("CommonName", "CommonPopName", "MetaComments")
      ) %>%
      mutate(
        TimeSeriesID = case_when(
          MetaComments == "STADEM, DABOM, and QRF" ~ TimeSeriesID_new,
          is.na(TimeSeriesID)                      ~ TimeSeriesID_new,
          TRUE                                     ~ TimeSeriesID
        )
      ) %>%
      select(-TimeSeriesID_new)
  } %>%
  # assign CompilerRecordID using TimeSeriesID and SpawningYear
  mutate(
    CompilerRecordID = if_else(
      StatusMA != "DELETE" & !is.na(TimeSeriesID),
      paste0(TimeSeriesID, "-", SpawningYear),
      ""
    )
  ) %>%
  arrange(CommonName, CommonPopName, SpawningYear)

#----------------------------------------
# some final modifications based on QA/QC
srfs_prep_4_cax2 = srfs_prep_4_cax %>%
  # in any case where the "Portion" estimate is 0, I don't want to report the expanded estimate.
  group_by(CommonName, CommonPopName, SpawningYear) %>%
  mutate(portion0 = any(PopFit == "Portion" & NOSAIJ == 0, na.rm = TRUE)) %>%
  filter(!(portion0 & PopFit == "Same")) %>% # if group Portion estimate is 0, remove the Same record
  mutate(BestValue = if_else(PopFit == "Portion" & NOSAIJ == 0, "Yes", BestValue)) %>% # and mark the Portion estimate as BestValue
  select(-portion0) %>%
  ungroup() %>%
  # add a comment for SF Clearwater populations using SC1 estimate
  mutate(
    Comments = if_else(
      CommonPopName %in% c("CRSFC-s", "SCUMA") & pop_sites == "SC1",
      paste0(
        "Estimate reflects escapement past SC1 which occurs downstream of the population boundary. ",
        coalesce(Comments, "")
      ),
      Comments
    )
  )

#-------------------------------------------------------------
# reorder and QC columns to follow CAX data exchange standards
source("R/nosa_des_spec.R")

# re-order & add missing columns
srfs_to_cax = apply_cax_des_col_order(srfs_prep_4_cax2, nosa_des_spec) %>%
  select(StatusMA, everything(), -pop_sites, -p_qrf)

# QC column types
qc_report = qc_against_des_spec(srfs_to_cax, nosa_des_spec)

# write to excel
write_xlsx(srfs_to_cax, path = paste0("output/SnakeRiverFishStatus_Results_4_CAX_NOSA_", Sys.Date(), ".xlsx"))

#---------------------------------
# final QC before submittal to CAX

# TimeSeriesID: does each group have only one TimeSeriesID; success = 0 rows
ts_qc_1 = srfs_to_cax %>%
  group_by(CommonName, CommonPopName, PopFit) %>%
  summarise(
    n_ts = n_distinct(TimeSeriesID),
    TimeSeriesIDs = paste(sort(unique(TimeSeriesID)), collapse = ", "),
    .groups = "drop"
  ) %>%
  filter(n_ts > 1)

# TimeSeriesID: does each TimeSeriesID map to only one group; success = 0 rows
ts_qc_2 = srfs_to_cax %>%
  group_by(TimeSeriesID) %>%
  summarise(
    n_groups = n_distinct(paste(CommonName, CommonPopName, PopFit, sep = " | ")),
    groups = paste(unique(paste(CommonName, CommonPopName, PopFit, sep = " | ")), collapse = "; "),
    .groups = "drop"
  ) %>%
  filter(n_groups > 1)

# PopID: make sure each CommonPopName is assigned its correct, unique PopID and matches what is in the CAX "Populations" table; success = 0 rows
pop_id_qc = srfs_to_cax %>%
  left_join(
    pop_df %>%
      select(CommonPopName = trt_pop_id, PopID_expected = id) %>%
      distinct(),
    by = "CommonPopName"
  ) %>%
  filter(is.na(PopID_expected) | PopID != PopID_expected) %>%
  select(CommonName, CommonPopName, PopID, PopID_expected)

### END SCRIPT
