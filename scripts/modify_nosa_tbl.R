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
  nosa_tbl = DBI::dbReadTable(con, "NOSA")
  pop_tbl = DBI::dbReadTable(con, "Populations")
  DBI::dbDisconnect(con)
  rm(con, connectNPTCAdbase)
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
         NMFS_Population,
         MajorPopGroup,
         PopStatus) %>%
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
pop_to_nosa = pop_esc_df %>%
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
      filter(site %in% c("PAHH", "RAPH")) %>%
      select(-site_operational) %>%
      rename(pop_sites = site) %>%
      mutate(mpg = case_when(
        species == "Chinook"   & pop_sites == "RAPH" ~ "South Fork Salmon River",
        species == "Chinook"   & pop_sites == "PAHH" ~ "Upper Salmon River",
        species == "Steelhead" & pop_sites == "RAPH" ~ "Salmon River",
        species == "Steelhead" & pop_sites == "PAHH" ~ "Salmon River"
      )) %>%
      mutate(popid = case_when(
        species == "Chinook"   & pop_sites == "RAPH" ~ "SRLSR",
        species == "Chinook"   & pop_sites == "PAHH" ~ "SRPAH",
        species == "Steelhead" & pop_sites == "RAPH" ~ "SRLSR-s",
        species == "Steelhead" & pop_sites == "PAHH" ~ "SRPAH-s"
      ))
  ) %>%
  # remove habitat expansion estimates
  select(-contains("hab_exp"))


  
  
  
  mutate(species = recode(species, "Chinook" = "Chinook salmon"))

