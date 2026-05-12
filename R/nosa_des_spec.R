# column order and specs from coordinated assessments data exhange standards
nosa_des_spec = tribble(
  ~field,                 ~rtype, ~max_chars,
  "ID",                   "chr",  36,
  "TimeSeriesID",         "int",  NA,
  "CommonName",           "chr",  50,
  "Run",                  "chr",  20,
  "RecoveryDomain",       "chr",  255,
  "ESUDPS",               "chr",  255,
  "MajorPopGroup",        "chr",  255,
  "PopID",                "int",  NA,
  "CommonPopName",        "chr",  255,
  "PopFit",               "chr",  8,
  "PopFitNotes",          "chr",  NA,   
  "EstimateType",         "chr",  10,
  "WaterBody",            "chr",  255,
  "EscapementLong",       "dbl",  NA,
  "EscapementLat",        "dbl",  NA,
  "SpawningYear",         "int",  NA,
  "EscapementTiming",     "chr",  7,
  "ContactAgency",        "chr",  255,
  "MethodNumber",         "int",  NA,
  "BestValue",            "chr",  13,
  # indicators
  "NOSAIJ",               "int",  NA,
  "NOSAIJLowerLimit",     "int",  NA,
  "NOSAIJUpperLimit",     "int",  NA,
  "NOSAIJAlpha",          "dbl",  NA,
  "NOSAEJ",               "int",  NA,
  "NOSAEJLowerLimit",     "int",  NA,
  "NOSAEJUpperLimit",     "int",  NA,
  "NOSAEJAlpha",          "dbl",  NA,
  "NOBroodStockRemoved",  "int",  NA,
  # metrics
  "pHOSij",               "dbl",  NA,
  "pHOSijLowerLimit",     "dbl",  NA,
  "pHOSijUpperLimit",     "dbl",  NA,
  "pHOSijAlpha",          "dbl",  NA,
  "pHOSej",               "dbl",  NA,
  "pHOSejLowerLimit",     "dbl",  NA,
  "pHOSejUpperLimit",     "dbl",  NA,
  "pHOSejAlpha",          "dbl",  NA,
  "NOSJF",                "dbl",  NA,
  "NOSJFLowerLimit",      "dbl",  NA,
  "NOSJFUpperLimit",      "dbl",  NA,
  "NOSJFAlpha",           "dbl",  NA,
  "HOSJF",                "dbl",  NA,
  "TSAIJ",                "int",  NA,
  "TSAIJLowerLimit",      "int",  NA,
  "TSAIJUpperLimit",      "int",  NA,
  "TSAIJAlpha",           "dbl",  NA,
  "TSAEJ",                "int",  NA,
  "TSAEJLowerLimit",      "int",  NA,
  "TSAEJUpperLimit",      "int",  NA,
  "TSAEJAlpha",           "dbl",  NA,
  # age distribution
  "Age2Prop",                 "dbl", NA,
  "Age2PropLowerLimit",       "dbl", NA,
  "Age2PropUpperLimit",       "dbl", NA,
  "Age3Prop",                 "dbl", NA,
  "Age3PropLowerLimit",       "dbl", NA,
  "Age3PropUpperLimit",       "dbl", NA,
  "Age4Prop",                 "dbl", NA,
  "Age4PropLowerLimit",       "dbl", NA,
  "Age4PropUpperLimit",       "dbl", NA,
  "Age5Prop",                 "dbl", NA,
  "Age5PropLowerLimit",       "dbl", NA,
  "Age5PropUpperLimit",       "dbl", NA,
  "Age6Prop",                 "dbl", NA,
  "Age6PropLowerLimit",       "dbl", NA,
  "Age6PropUpperLimit",       "dbl", NA,
  "Age7Prop",                 "dbl", NA,
  "Age7PropLowerLimit",       "dbl", NA,
  "Age7PropUpperLimit",       "dbl", NA,
  "Age8Prop",                 "dbl", NA,
  "Age8PropLowerLimit",       "dbl", NA,
  "Age8PropUpperLimit",       "dbl", NA,
  "Age9Prop",                 "dbl", NA,
  "Age9PropLowerLimit",       "dbl", NA,
  "Age9PropUpperLimit",       "dbl", NA,
  "Age10Prop",                "dbl", NA,
  "Age10PropLowerLimit",      "dbl", NA,
  "Age10PropUpperLimit",      "dbl", NA,
  "Age11PlusProp",            "dbl", NA,
  "Age11PlusPropLowerLimit",  "dbl", NA,
  "Age11PlusPropUpperLimit",  "dbl", NA,
  "AgePropAlpha",             "dbl", NA,
  # protocol and method documentation
  "ProtMethName",         "chr",  NA,   
  "ProtMethURL",          "chr",  NA,   
  "ProtMethDocumentation","chr",  NA,   
  "MethodAdjustments",    "chr",  NA,   
  "OtherDataSources",     "chr",  255,
  # supporting information
  "Comments",             "chr",  NA,   
  "NullRecord",           "chr",  3,
  "DataStatus",           "chr",  255,
  "IndicatorLocation",    "chr",  NA,   
  "MetricLocation",       "chr",  NA,   
  "MeasureLocation",      "chr",  NA,   
  "ContactPersonFirst",   "chr",  30,
  "ContactPersonLast",    "chr",  30,
  "ContactPhone",         "chr",  30,
  "ContactEmail",         "chr",  50,
  "MetaComments",         "chr",  NA,
  # appendix a (doesn't include all columns there)
  "SubmitAgency",         "chr",  15,
  "RefID",                "int",  NA,
  "UpdDate",             "dttm",  NA,
  "DataEntry",            "chr",  50,
  "DataEntryNotes",       "chr",  NA,
  "Publish",              "chr",  3,
  "CompilerRecordID",     "chr",  36
)

# function to apply column orders according to cax data exchange standards
apply_cax_des_col_order <- function(df, nosa_des_spec) {
  nosa_fields <- nosa_des_spec$field
  
  # add missing fields using the specified rtype
  missing <- setdiff(nosa_fields, names(df))
  if (length(missing) > 0) {
    miss_spec <- nosa_des_spec %>% dplyr::filter(field %in% missing)
    
    for (i in seq_len(nrow(miss_spec))) {
      f <- miss_spec$field[i]
      t <- miss_spec$rtype[i]
      
      df[[f]] <- switch(
        t,
        chr = rep("", nrow(df)),
        dbl = rep(NA_real_, nrow(df)),
        num = rep(NA_real_, nrow(df)),
        int = rep(NA_integer_, nrow(df)),
        # fallback if an unexpected rtype shows up
        rep(NA, nrow(df))
      )
    }
  }
  
  # reorder columns: DES order first, then any extra columns I had
  df %>% dplyr::select(dplyr::any_of(nosa_fields), dplyr::everything())
}

# function to qc columns against cax data exchange standards
qc_against_des_spec = function(df, nosa_des_spec) {
  nosa_fields = nosa_des_spec$field
  
  # helper: "actual type" in a simple label
  actual_type <- function(x) {
    if (inherits(x, "integer")) "int"
    else if (inherits(x, "numeric")) "dbl"
    else if (inherits(x, "character")) "chr"
    else class(x)[1]
  }
  
  # check each field
  qc = pmap_dfr(nosa_des_spec, function(field, rtype, max_chars) {
    if (!field %in% names(df)) {
      return(tibble(
        field = field,
        issue = "missing_column",
        expected = rtype,
        actual = NA_character_,
        n_bad = NA_integer_
      ))
    }
    
    x = df[[field]]
    act = actual_type(x)
    
    # type compatibility rules (simple + practical):
    # - chr expected: require character
    # - dbl expected: allow integer or numeric
    # - int expected: allow integer OR numeric but must be whole-valued
    issues = list()
    
    if (rtype == "chr") {
      if (!is.character(x)) {
        issues = append(issues, list(tibble(
          field = field, issue = "wrong_type", expected = "chr", actual = act, n_bad = NA_integer_
        )))
      } else if (!is.na(max_chars)) {
        bad = sum(!is.na(x) & nchar(x) > max_chars)
        if (bad > 0) {
          issues = append(issues, list(tibble(
            field = field, issue = "max_chars_exceeded", expected = paste0("<= ", max_chars),
            actual = paste0("max nchar=", max(nchar(x), na.rm = TRUE)), n_bad = bad
          )))
        }
      }
    }
    
    if (rtype %in% c("dbl","num")) {
      if (!(is.numeric(x) || is.integer(x))) {
        issues = append(issues, list(tibble(
          field = field, issue = "wrong_type", expected = "dbl", actual = act, n_bad = NA_integer_
        )))
      }
    }
    
    if (rtype == "int") {
      if (!(is.numeric(x) || is.integer(x))) {
        issues = append(issues, list(tibble(
          field = field, issue = "wrong_type", expected = "int", actual = act, n_bad = NA_integer_
        )))
      } else {
        xnum = suppressWarnings(as.numeric(x))
        bad = sum(!is.na(xnum) & abs(xnum - round(xnum)) > 1e-9)
        if (bad > 0) {
          issues = append(issues, list(tibble(
            field = field, issue = "non_integer_values", expected = "whole numbers",
            actual = act, n_bad = bad
          )))
        }
      }
    }
    
    if (length(issues) == 0) {
      tibble(field = field, issue = "ok", expected = rtype, actual = act, n_bad = 0L)
    } else {
      bind_rows(issues)
    }
  }) %>%
    filter(issue != "ok")
  
  qc
}

