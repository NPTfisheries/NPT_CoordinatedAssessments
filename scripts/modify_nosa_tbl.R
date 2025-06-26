# -----------------------
# Author(s): Mike Ackerman
# Purpose: 
# 
# Created Date: June 26, 2025
#   Last Modified: 
#
# Notes:

# clear environment
rm(list = ls())

# load packages
library(DBI)
library(odbc)
library(tidyverse)
library(here)
library(readxl)
library(sf)

# location of NPT Coordinated Assessments database
path_to_db = here("data/StreamNet API interface DES version 2024.1 - NPT.accdb")

# connect to database, load NOSA table, and disconnect
if(!is.null(path_to_db)) {
  source(here("R/connectNPTCAdbase.R"))
  con = connectNPTCAdbase(path_to_db)
  nosa_tbl = DBI::dbReadTable(con, "NOSA-RK")
  npt_dbo_nosa_tbl = DBI::dbReadTable(con, "dbo_NOSA")
  pop_tbl = DBI::dbReadTable(con, "Populations")
  DBI::dbDisconnect(con)
}

# clean up pop_tbl
sr_pop_tbl = pop_tbl %>%
  filter(str_detect(ESU_DPS, "Snake River"),
         str_detect(CommonName, "Chinook|Steelhead"),
         !is.na(TRT_POP_ID),
         !PopStatus == "Extirpated") %>%
  select(CommonName,
         TRT_POP_ID,
         Run,
         ESU_DPS,
         MajorPopGroup,
         PopID,
         RecoveryDomain) %>%
  arrange(CommonName, MajorPopGroup, TRT_POP_ID)

# get coordinates for int and mrr sites
load("C:/Git/SnakeRiverFishStatus/data/configuration_files/site_config_LGR_20250416.rda") ; rm(configuration, parent_child, flowlines, sr_site_pops)
site_ll = crb_sites_sf %>%
  select(site_code) %>%
  st_transform(4326) %>%
  mutate(
    EscapementLong = st_coordinates(.)[, 1],
    EscapementLat  = st_coordinates(.)[, 2]
  )

# read in population escapement estimates
pop_esc_df = list.files("C:/Git/SnakeRiverFishStatus/output/syntheses",
                    pattern = "(Chinook|Steelhead).*\\.xlsx$",
                    full.names = TRUE) %>%
  discard(~ grepl("~\\$", basename(.x))) %>%  # exclude temp/lock files, if an issue
  map_dfr(~ read_excel(.x, sheet = "Pop_Tot_Esc"))

# read in site escapement estimates
site_esc_df = list.files("C:/Git/SnakeRiverFishStatus/output/syntheses",
                        pattern = "(Chinook|Steelhead).*\\.xlsx$",
                        full.names = TRUE) %>%
  discard(~ grepl("~\\$", basename(.x))) %>%  # exclude temp/lock files, if an issue
  map_dfr(~ read_excel(.x, sheet = "Site_Esc"))

# prep pop_esc_df to export in same format as nosa_tbl
pop_esc_to_nosa = pop_esc_df %>%
  # CRSFC-s
  filter(popid != "CRSFC-s") %>%
  mutate(
    popid = if_else(popid == "CRLMA-s/CRSFC-s", "CRSFC-s", popid),
    p_qrf_hab = if_else(popid == "CRSFC-s", 1, p_qrf_hab)
  ) %>%
  # SCUMA
  filter(popid != "SCUMA") %>%
  mutate(
    popid = if_else(popid == "SCLAW/SCUMA", "SCUMA", popid),
    p_qrf_hab = if_else(popid == "SCUMA", 1, p_qrf_hab)
  ) %>%
  # remove some estimates that cover multiple pops
  filter(!popid %in% c("GRCAT/GRUMA", "GRLOS/GRMIN", "IRMAI/IRBSH", "SEUMA/SEMEA/SEMOO", "SFMAI-s/SFSEC-s", "SFSMA/SFSEC/SFEFS",
                       "SRLMA/SRPAH/SREFS/SRYFS/SRVAL/SRUMA", "SRPAH-s/SREFS-s/SRUMA-s")) %>%
  # toss out Tucannon estimates
  filter(!str_detect(popid, "SNTUC")) %>%
  # add estimates from RAPH and PAHH
  bind_rows(
    site_esc_df %>%
      filter(site %in% c("PAHH", "RAPH") &
               !(species == "Steelhead" & site == "RAPH")) %>%
      select(-site_operational) %>%
      rename(pop_sites = site) %>%
      mutate(mpg = case_when(
        species == "Chinook"   & pop_sites == "RAPH" ~ "South Fork Salmon River",
        species == "Chinook"   & pop_sites == "PAHH" ~ "Upper Salmon River",
        species == "Steelhead" & pop_sites == "PAHH" ~ "Salmon River"
      )) %>%
      mutate(popid = case_when(
        species == "Chinook"   & pop_sites == "RAPH" ~ "SRLSR",
        species == "Chinook"   & pop_sites == "PAHH" ~ "SRPAH",
        species == "Steelhead" & pop_sites == "PAHH" ~ "SRPAH-s"
      )) %>%
      mutate(p_qrf_hab = case_when(
        species == "Chinook"   & pop_sites == "RAPH" ~ 0.26,
        species == "Chinook"   & pop_sites == "PAHH" ~ 0.32,
        species == "Steelhead" & pop_sites == "PAHH" ~ 0.99
      ))
  ) %>%
  # remove habitat expansion estimates and columns not used in coordinated assessments
  select(-contains("hab_exp"), 
         -incl_sites, 
         -n_tags, 
         -mpg, 
         -mean,
         -mode,
         -sd,
         -cv) %>%
  # recode SFSMA to SFMAI
  mutate(popid = if_else(popid == "SFSMA", "SFMAI", popid)) %>%
  # join population information from CA population table
  mutate(species = recode(species, "Chinook" = "Chinook salmon")) %>%
  left_join(sr_pop_tbl,
            by = c("species" = "CommonName", "popid" = "TRT_POP_ID")) %>%
  # grab a lat/lon from the first site in pop_sites
  mutate(site_code = str_extract(pop_sites, "^[^,]+")) %>%
  left_join(site_ll %>%
              st_drop_geometry()) %>%
  select(-site_code) %>%
  # rename some columns to match nosa_tbl
  rename(CommonName = species,
         SpawningYear = spawn_yr,
         CommonPopName = popid,
         NOSAIJ = median,
         NOSAIJLowerLimit = lower95ci,
         NOSAIJUpperLimit = upper95ci) %>%
  # add PopFit info
  mutate(PopFit = if_else(p_qrf_hab >= 0.9, "Same", "Portion"),
         PopFitNotes = paste0(round(p_qrf_hab * 100, 1), "; Estimate reflects escapement to PTAGIS sites: ", pop_sites,". Percent pop coverage estimated using redd QRF (See et al. 2021) dataset. PopFit 'Same' if >= 90.0."),
         Comments = notes) %>%
  select(-pop_sites, -p_qrf_hab, -notes) %>%
  # add non-overlapping columns from nosa_tbl (safe method)
  {
    join_keys <- c("CommonName", "CommonPopName", "SpawningYear")
    cols_to_add <- setdiff(names(nosa_tbl), c(names(.), join_keys))
    left_join(., select(nosa_tbl %>% mutate(CommonPopName = if_else(CommonPopName == "SFSMA", "SFMAI", CommonPopName)), all_of(join_keys), all_of(cols_to_add)), by = join_keys)
  } %>%
  # fill in some columns unique to each species and population
  group_by(CommonName, CommonPopName) %>%
  fill(TimeSeriesID, WaterBody, .direction = "downup") %>%
  ungroup() %>%
  # fix ProtMethName
  mutate(ProtMethName = "PIT tag Based Escapement Estimation Above Lower Granite Dam v1.0") %>%
  # add EmigrationTiming based on species %>%
  mutate(EscapementTiming = case_when(
    CommonName == "Chinook salmon" ~ "Jun-Oct",
    CommonName == "Steelhead" ~ "Feb-Jun"
  )) %>%
  # fill in some remaining columns
  fill(EstimateType, 
       ContactAgency, 
       MethodNumber,
       BestValue,
       NOSAIJAlpha,
       ProtMethURL,
       NullRecord,
       DataStatus,
       MeasureLocation,
       ContactPersonFirst,
       ContactPersonLast,
       ContactPhone,
       ContactEmail,
       MetaComments,
       SubmitAgency,
       Publish,
       .direction = "downup") %>%
  # re-fill CompilerRecordID
  mutate(CompilerRecordID = paste0(TimeSeriesID, "-", SpawningYear)) %>%
  # remove records that already exist in npt_dbo_nosa_tbl
  anti_join(npt_dbo_nosa_tbl,
            by = c("PopID", "PopFit", "WaterBody", "SpawningYear", "ContactAgency", "MethodNumber")) %>%
  # finally, ensure columns are in the same order as the original nosa_tbl
  select(any_of(names(nosa_tbl))) %>%
  mutate(across(
    names(.)[names(.) %in% names(nosa_tbl)],
    ~ {
      colname = cur_column()
      target_type = class(nosa_tbl[[colname]])[1]
      # coerce to same class as in nosa_tbl
      match.fun(paste0("as.", target_type))(.)
    }
  ))

#------------------
# replace NOSA table in Access DB (for unknown reasons, I needed to use RODBC instead of DBI to write the table)
library(RODBC)

# re-connect to access db
channel = odbcConnectAccess2007(here("data/StreamNet API interface DES version 2024.1 - NPT.accdb"))

# remove the existing NOSA table (I previously copied the original over to a NOSA-RK table)
sqlDrop(channel, "NOSA", errors = FALSE)

# write pop_esc_to_nosa to db
sqlSave(channel, pop_esc_to_nosa, tablename = "NOSA", rownames = FALSE)

# dis-connect access db
close(channel)

# NOTE: After pushing pop_esc_to_nosa to the NOSA table in the access database, I needed to make the following changes to data formats
# in the table:
# EscapementLong: Field Size = Single
# EscapementLat: Field Size = Single
# NOSAIJAlpha: Field Size = Single
# CompilerRecordID: Required = Yes, Indexed = Yes (No Duplicates)

### END SCRIPT
