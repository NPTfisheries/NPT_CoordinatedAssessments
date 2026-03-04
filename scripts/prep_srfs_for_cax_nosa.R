# -----------------------
# Author(s): Mike Ackerman
# Purpose: Prepare latest SnakeRiverFishStatus natural-origin spawner abundance (NOSA) estimates for upload to Coordinated Assessments (CAX) database. 
#   The goal is to replace all past NOSA estimates uploaded by NPT with the latest and provide them in a standardized manner that makes clear
#   IPTDS-based escapement estimates versus expanded spawner abundance estimates (accounting for unmonitored habitat). 
# 
# Created Date: January 23, 2026
#   Last Modified: February 18, 2026
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

#---------------------------------------
# read in SnakeRiverFishStatus results
srfs_results_files = list.files("data/SRFS",
                                pattern = "(Chinook|Steelhead).*\\.xlsx$",
                                full.names = TRUE) %>%
  discard(~ grepl("~\\$", basename(.x)))
  
# population escapement estimates
pop_esc_df = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Pop_Tot_Esc"))

# population age proportions
age_p_df = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Pop_Age_Props"))

# site escapements
site_esc_df = map_dfr(srfs_results_files, ~ read_excel(.x, sheet = "Site_Esc"))

#----------------------------------------
# retrieve locations for PTAGIS INT sites
site_ll = queryInterrogationMeta() %>%
  select(site_code = siteCode, latitude, longitude) %>%
  bind_rows(queryMRRMeta() %>%
              select(site_code = siteCode, latitude, longitude)) %>%
  filter(!is.na(latitude) | !is.na(longitude)) %>%
  st_as_sf(
    coords = c("longitude", "latitude"),
    crs = 4326
  ) %>%
  mutate(
    EscapementLong = st_coordinates(.)[, 1],
    EscapementLat  = st_coordinates(.)[, 2]
  )

#----------------------------------
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

npt_cax_nosa = cax_nosa %>%
  filter(submitagency == "NPT")

# datasets available in CAX
cax_datasets = rcax_datasets()

# load populations table from CAX
pop_df = rcax_table_query(tablename = "Populations")

# clean up pop_df
sr_pop_df = pop_df %>%
  filter(str_detect(esudps, "Snake River"),
         str_detect(commonname, "Chinook|Steelhead"),
         !is.na(trt_pop_id),
         trt_pop_id != "",
         !popstatus == "Extirpated") %>%
  select(CommonName      = commonname,
         CommonPopName   = trt_pop_id,
         Run             = run,
         RecoveryDomain  = recoverydomain,
         MajorPopGroup   = majorpopgroup,
         PopID           = id,           
         NMFS_POPID      = nmfs_popid,
         LocationName    = locationname,   # same as nmfs_population
         PopulationName  = populationname,
         ESApopName      = esapopname,
         ESU_DPS         = esudps,
         NMFS_Population = nmfs_population) %>%
  arrange(CommonName, MajorPopGroup, CommonPopName)

#-----------------------------------------------------------------
# prep age_p_df to join to abundance results and formatted for CAX
age_p_for_cax = age_p_df %>%
  select(species:upper95ci) %>%
  mutate(across(c(median, lower95ci, upper95ci), ~ signif(round(.x, 8), 8)),
         param = str_remove(param, "^p_"),
         param = str_replace(param, "age_", "Age")) %>%
  group_by(species, spawn_yr, popid) %>%
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
  ) %>%
  ungroup()

#----------------------------------------------------------------
# prep SnakeRiverFishStatus results to compare and upload to NOSA

# the threshhold on which to consider a pop fully monitored by IPTDS, what do we want to set this at? 
threshhold = 0.90

# prep SnakeRiverFishStatus results for CAX NOSA table
srfs_to_cax = pop_esc_df %>%
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
  filter(!str_detect(CommonPopName, "/"),                            ### we could submit these as PopFit = Multiple, which would require additional work
         !str_detect(CommonPopName, "SNTUC")) %>%
  # do we want to try to add estimates from RAPH and PAHH? if so, these would need to be QC'd
  # bind_rows(
  #   site_esc_df %>%
  #     filter(site %in% c("PAHH", "RAPH") & !(species == "Steelhead" & site == "RAPH")) %>%
  #     select(-site_operational) %>%
  #     rename(pop_sites = site, CommonName = species) %>%
  #     mutate(mpg = case_when(
  #       CommonName == "Chinook"   & pop_sites == "RAPH" ~ "South Fork Salmon River",
  #       CommonName == "Chinook"   & pop_sites == "PAHH" ~ "Upper Salmon River",
  #       CommonName == "Steelhead" & pop_sites == "PAHH" ~ "Salmon River"
  #     )) %>%
  #     mutate(CommonPopName = case_when(
  #       CommonName == "Chinook"   & pop_sites == "RAPH" ~ "SRLSR",
  #       CommonName == "Chinook"   & pop_sites == "PAHH" ~ "SRPAH",
  #       CommonName == "Steelhead" & pop_sites == "PAHH" ~ "SRPAH-s"
  #     )) %>%
  #     mutate(p_qrf = case_when(
  #       CommonName == "Chinook"   & pop_sites == "RAPH" ~ 0.26,
  #       CommonName == "Chinook"   & pop_sites == "PAHH" ~ 0.32,
  #       CommonName == "Steelhead" & pop_sites == "PAHH" ~ 0.99
  #     ))
  # ) %>%
  # attach population metadata from sr_pop_df
  mutate(CommonName = recode(CommonName, "Chinook" = "Chinook Salmon")) %>%
  left_join(sr_pop_df, by = c("CommonName", "CommonPopName")) %>%
  # add lat/lon based on the first site in pop_sites
  mutate(site_code = str_extract(pop_sites, "^[^,]+")) %>%
  left_join(site_ll %>% st_drop_geometry(), by = "site_code") %>%
  select(-site_code) %>%
  # set PopFit for median, lower95ci, and upper95ci based on proportion of habitat monitored; expanded ests will be added below
  mutate(PopFit = if_else(p_qrf >= threshhold, "Same", "Portion"),
         MetaComments = "STADEM and DABOM") %>%
  {
    df = .
    bind_rows(
      df,
      df %>%
        filter(p_qrf < threshhold & PopFit == "Portion") %>%
        mutate(median       = median_exp,
               lower95ci    = lower95ci_exp,
               upper95ci    = upper95ci_exp,
               PopFit       = "Same",
               MetaComments = "STADEM, DABOM, and QRF")
    )
  } %>%
  # expanded ests no longer needed
  select(-contains("_exp")) %>%
  # round estimates and rename, add alpha
  mutate(across(c(median, lower95ci, upper95ci), round)) %>%
  rename(NOSAIJ           = median,
         NOSAIJLowerLimit = lower95ci,
         NOSAIJUpperLimit = upper95ci) %>%
  mutate(NOSAIJAlpha      = 0.05) %>%
  # assign EstimateType: Escapement if IPTDS-based, NOSA if expanded
  mutate(EstimateType = case_when(
    MetaComments == "STADEM and DABOM"       ~ "Escapement",
    MetaComments == "STADEM, DABOM, and QRF" ~ "NOSA",
    TRUE                                     ~ NA_character_
  )) %>%
  # assign EscapementTiming by species
  mutate(EscapementTiming = case_when(
    CommonName == "Chinook Salmon" ~ "Jun-Oct",
    CommonName == "Steelhead"      ~ "Feb-Jun",
    TRUE                           ~ NA_character_
  )) %>%
  # add PopFitNotes
  mutate(
    p_qrf       = round(p_qrf * 100, 1),
    site_note   = paste0("Estimate reflects PTAGIS site(s): ", pop_sites, " which monitor an estimated ", p_qrf, "% of available habitat."),
    qrf_note    = "Percent of available habitat monitored estimated using redd QRF dataset (See et al. 2021).",
    PopFitNotes = case_when(
      MetaComments == "STADEM and DABOM"       & PopFit == "Same"    ~ paste0(site_note, "PopFit considered 'Same' because >= ", threshhold * 100, "%. ", qrf_note),
      MetaComments == "STADEM and DABOM"       & PopFit == "Portion" ~ paste0(site_note, "PopFit considered 'Portion' because < ", threshhold * 100, "%. ", qrf_note),
      MetaComments == "STADEM, DABOM, and QRF" & PopFit == "Same"    ~ paste0(site_note, "PopFit considered 'Same' because 'Portion' escapement estimate was expanded to account for unmonitored habitat."),
      TRUE ~ NA_character_
    )
  ) %>%
  # as default, set BestValue to be "Yes" for "Same" estimates
  mutate(BestValue = if_else(PopFit == "Same", "Yes", "No")) %>%
  select(-p_qrf, -site_note, -qrf_note) %>%
  # add ID for records to update or delete (i.e., already in CAX)
  full_join(npt_cax_nosa %>%
              select(CommonName = commonname,
                     CommonPopName = commonpopname,
                     SpawningYear = spawningyear,
                     MetaComments = metacomments,
                     ID = id),
            by = c("CommonName", "CommonPopName", "SpawningYear", "MetaComments")) %>%
  filter(MetaComments %in% c("STADEM and DABOM", "STADEM, DABOM, and QRF")) %>%
  # provide my call for add, update, or delete
  mutate(StatusMA = case_when(
    is.na(NOSAIJ)  & !is.na(ID) ~ "DELETE",
    !is.na(NOSAIJ) & !is.na(ID) ~ "UPDATE",
    !is.na(NOSAIJ) & is.na(ID)  ~ "NEW"
  )) %>%
  select(StatusMA, everything()) %>%
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
    ProtMethDocumentation = paste0("See, K.E., R.N. Kinzer, and M.W. Ackerman. 2021. State-Space Model to Estimate Salmon Escapement Using Multiple Data Sources. North American Journal of Fisheries Management. DOI: 10.1002/nafm.10649; ",
                                   "Waterhouse, L., J. White, K. See, A. Murdoch, and B.X. Semmens. 2020. A Bayesian Nested Patch Occupancy Model to Estimate Steelhead Movement and Abundance. Ecological Applications 00(00):e02202. 10.1002/eap.2202; ",
                                   "Kinzer, R., R. Orme, M. Campbell, J. Hargrove, and K. See. 2020. Report to NOAA Fisheries for 5-year ESA Status Review: Snake River Basin Steelhead and Chinook Salmon Population Abundance, Life History, and Diversity Metrics Calculated from In-Stream PIT-Tag Observations (SY2010-SY2019). In-stream PIT-tag Detection Systems Workgroup. 118 pp.; ",
                                   "Ackerman et al. (In Prep).")
  ) %>%
  # assign WaterBody based on pop_sites
  mutate(WaterBody = case_when(
    pop_sites %in% c("ACB", "ACM")                               ~ "Asotin Creek",
    pop_sites == "BRC"                                           ~ "Bear Valley Creek",
    pop_sites == "TAY"                                           ~ "Big Creek",
    pop_sites %in% c("BBA", "KHS")                               ~ "Big Bear Creek",
    pop_sites %in% c("BSC", "BSC, CMP")                          ~ "Big Sheep Creek",
    pop_sites == "CCW"                                           ~ "Catherine Creek",
    pop_sites == "CLC"                                           ~ "Clear Creek",
    pop_sites == "ESS"                                           ~ "East Fork South Fork Salmon River",
    pop_sites == "UGR"                                           ~ "Grande Ronde River",
    pop_sites %in% c("COC, IR1", "IR3")                          ~ "Imnaha River",
    pop_sites == "JOC"                                           ~ "Joseph Creek",
    pop_sites == "LAP"                                           ~ "Lapwai Creek",
    pop_sites == "LLR"                                           ~ "Lemhi River",
    pop_sites == "LRL"                                           ~ "Lochsa River",
    pop_sites == "LC1"                                           ~ "Lolo Creek",
    pop_sites == "LGW"                                           ~ "Lookingglass Creek",
    pop_sites == "MAR"                                           ~ "Marsh Creek",
    pop_sites == "MR1"                                           ~ "Minam River",
    pop_sites == "MIS"                                           ~ "Mission Creek",
    pop_sites == "NFS"                                           ~ "North Fork Salmon River",
    pop_sites == "PCA"                                           ~ "Panther Creek",
    pop_sites %in% c("HLM", "JUL", "KHS, PCM")                   ~ "Potlatch River",
    pop_sites == "ZEN"                                           ~ "Secesh River",
    pop_sites == "SW1"                                           ~ "Selway River",
    pop_sites == "SC1"                                           ~ "South Fork Clearwater River",
    pop_sites == "KRS"                                           ~ "South Fork Salmon River",
    pop_sites %in% c("MDC, UGS", "UGS")                          ~ "upper Grande Ronde River",
    pop_sites == "VC2"                                           ~ "Valley Creek",
    pop_sites %in% c("MR1, WR2", "WR1", "WR2")                   ~ "Wallowa River",
    pop_sites == "WEN"                                           ~ "Wenaha River",
    pop_sites == "WB1"                                           ~ "Whitebird Creek",
    pop_sites == "YFK"                                           ~ "Yankee Fork Salmon River",
    pop_sites %in% c("ACM, COU", "CRC, LLR", "ESS, KRS",
                     "BED, CLC, LAP", "BED, EPR, LAP, LAW",
                     "CLC, HLM, KHS, LAP", "CLC, KHS, LAP, PCM", 
                     "HLM, KHS, LAP", "HLM, MIS", "JA1, LAP",
                     "JA1, SWT", "JUL, LAP", "KHS, LAP",
                     "LAP, LAW, SIX", "LGW, UGR", "VC2, YFK")    ~ "Multiple",
    TRUE ~ NA_character_
  )) %>%
  # for expanded estimates, just set WaterBody to "Multiple"
  mutate(WaterBody = if_else(MetaComments == "STADEM, DABOM, and QRF", "Multiple", WaterBody)) %>%
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
         DataStatus         = "Draft",
         # do IndicatorLocation, MetricLocation, MeasureLocation need to be changed to something other than CDMS?
         IndicatorLocation  = "npt-cdms.nezperce.org",
         MetricLocation     = "npt-cdms.nezperce.org",
         MeasureLocation    = "npt-cdms.nezperce.org",
         OtherDataSources   = "IDFG, ODFW, WDFW, Biomark, QCI, SBT, CTUIR",
         Publish            = "Yes") %>%
  arrange(CommonName, CommonPopName, SpawningYear)

#----------------------------------------
# some final modifications based on QA/QC
srfs_to_cax_qc = srfs_to_cax %>%
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
      "Estimate reflects escapement past SC1 which occurs downstream of the population boundary. ",
      Comments
    )
  )

# NOTE: Need to remove pop_sites before final export

# write to excel, if needed
write_xlsx(srfs_to_cax_qc, path = paste0("output/SnakeRiverFishStatus_Results_4_CAX_NOSA_", Sys.Date(), ".xlsx"))

#----------------------------------------------------------------------
# compare prepped SRFS results to existing CAX records submitted by NPT
comp_df = srfs_to_cax %>%
  select(CommonName,
         CommonPopName,
         SpawningYear,
         MetaComments,
         NOSAIJ) %>%
  rename(NOSAIJ_SRFS = NOSAIJ) %>%
  full_join(
    npt_cax_nosa %>%
      select(CommonName = commonname,
             CommonPopName = commonpopname,
             SpawningYear = spawningyear,
             MetaComments = metacomments,
             ID = id,
             NOSAIJ = nosaij) %>%
      rename(NOSAIJ_CAX = NOSAIJ),
    by = c("CommonName", "CommonPopName", "SpawningYear", "MetaComments")
  ) %>%
  mutate(
    source = case_when(
      !is.na(NOSAIJ_SRFS) & !is.na(NOSAIJ_CAX) ~ "BOTH",
      !is.na(NOSAIJ_SRFS) &  is.na(NOSAIJ_CAX) ~ "SRFS_ONLY",
       is.na(NOSAIJ_SRFS) & !is.na(NOSAIJ_CAX) ~ "CAX_ONLY",
      TRUE ~ NA_character_
    )
  )

# tentative records to delete in CAX; these records exist in CAX but I no longer report from SRFS
records_to_delete = comp_df %>%
  filter(MetaComments == "STADEM and DABOM", 
         source       == "CAX_ONLY")

### END SCRIPT




