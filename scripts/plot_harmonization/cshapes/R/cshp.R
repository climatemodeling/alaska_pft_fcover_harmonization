##' Access the CShapes dataset in R
##'
##' @param date The date for which the cshapes polygons should be extracted. This argument must be of type Date and must be in the range 1/1/1886 - end of the dataset. If omitted, the complete dataset is returned.
##' @param useGW Boolean argument specifying the system membership coding. TRUE: Gleditsch and Ward (GW, default). FALSE: Correlates of War (COW).
##' @param dependencies Boolean argument specifying whether dependent territories must be included. TRUE: Returns polygons for both independent states and dependent units. FALSE: Returns polygons for indepdendent states only (default).
##' @return A \code{sf} dataframe containing the complete CShapes dataset, or the CShapes snapshot for the specified date.
##'
#' @importFrom sf st_read st_transform st_set_crs
##' @export
##'
cshp <- function(date = NA, useGW = TRUE, dependencies = FALSE) {
  defineProj <- function(sf){sf::st_transform(sf::st_set_crs(sf, 4326), 4326)}

  ## Errors and warnings
  if (!is.na(date) && !inherits(date, "Date")) {
    stop("date is not of type Date")
  }

  if (!is.na(date) && (date < as.Date("1886-01-01") |
                       date > as.Date("2019-12-31"))) {
    stop("Specified date is out of range")
  }

  ## Get polygons
  if (useGW == TRUE) {
    fpath <- system.file("extdata", "cshapes_2_gw.topojson.xz", package="cshapes")
  } else {
    fpath <- system.file("extdata", "cshapes_2_cow.topojson.xz", package="cshapes")
  }
  fcontent <- readLines(fpath, warn = FALSE, encoding = "UTF8")
  cshp.full <- defineProj(sf::st_read(fcontent, quiet = TRUE, drivers = "TopoJSON"))

  ## Remove redundant columns
  cs.cols <- c("gwcode", "cowcode", "country_name", "start", "end", "status",
               "owner", "capname", "caplong", "caplat", "b_def", "fid", "geometry")
  cshp.full <- cshp.full[,names(cshp.full) %in% cs.cols]

  ## Remove or keep dependencies
  if (dependencies == TRUE){
    cshp.part <- cshp.full
  } else {
    cshp.part <- cshp.full[cshp.full$status == "independent",]
  }

  ## Subset to date or return full dataset
  if(is.na(date)){
    return(cshp.part)
  } else {
    cshp.part <- cshp.part[as.Date(cshp.part$start) <= date &
                             as.Date(cshp.part$end) >= date,]
    return(cshp.part)
  }
}
