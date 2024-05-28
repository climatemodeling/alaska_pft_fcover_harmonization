##' Generate between-country distance matrix on the CShapes dataset
##'
##' This function returns between-country distances in km for the given date. Output format is a distance matrix that lists distances between each pair of countries.
##' The function can compute different types of distance lists, specified by the \code{type} parameter:
##' \enumerate{
##'   \item Capital distances
##'   \item Centroid distances
##'   \item Minimum distances between polygons
##' }
##' The latter computation is very expensive if polygons consist of many points.
##' For that reason, the function simplifies the country polygons according to the Douglas-Peucker algorithm
##' (http://en.wikipedia.org/wiki/Ramer-Douglas-Peucker_algorithm), which eliminates points from the polygons and speeds up computation.
##' The \code{keep} parameter specifies the proportion of points to retain in the simplified country polygons.
##'
##' @param date The date for which the distance matrix should be computed. This argument must be of type Date and must be in the range 1/1/1886 - end of the dataset.
##' @param type Specifies the type of distance matrix: "capdist" for capital distances, "centdist" for centroid distances, and "mindist" for minimum distances.
##' @param keep Proportion of points to retain following polygon simplification using Douglas-Peucker algorithm. Default: 0.1. See package \code{rmapshaper}.
##' @param useGW Boolean argument specifying the system membership coding. TRUE: Gleditsch and Ward (GW, default). FALSE: Correlates of War (COW).
##' @param dependencies Boolean argument specifying whether dependent territories must be included. TRUE: Returns polygons for both independent states and dependent units. FALSE: Returns polygons for indepdendent states only (default).
##' @return A quadratic weights matrix, with the row and column labels containing the country identifiers in the specified coding system (COW or GW).
##'
##' @importFrom rmapshaper ms_simplify
##' @export

distmatrix <- function(date, type = "mindist", keep = 0.1, useGW = TRUE, dependencies = FALSE){

  ## Errors and warnings
  if (!is.na(date) && !inherits(date, "Date")) {
    stop("date is not of type Date")
  }

  if (!is.na(date) && (date < as.Date("1886-01-01") |
                         date > as.Date("2019-12-31"))) {
    stop("Specified date is out of range")
  }

  if (!(type %in% c("mindist", "capdist", "centdist"))) {
    stop("Wrong type argument. Possible values: mindist, capdist, centdist")
  }

  if (keep <= 0){
    warning("Douglas Peucker simplification algorithm: \n
    'keep' (proportion of points to retain) must be > 0")
  }

  ## Call cshp function (COW or GW)
  cshp.part <- cshp(date, useGW = useGW, dependencies = dependencies)

  ## Get vector of all country codes
  if ("gwcode" %in% names(cshp.part)){
    ccodes <- cshp.part$gwcode
  } else {
    ccodes <- cshp.part$cowcode
  }

  ## Minimal distance between polygons
  if (type == "mindist"){
    cshp.simple <- rmapshaper::ms_simplify(cshp.part, keep_shapes=T, keep=0.1,
                               method = "dp",  snap=T)
    resultmatrix <- matrix(0, nrow = length(ccodes), ncol = length(ccodes))

    for (c1 in 1:(length(ccodes) - 1)) {
      for (c2 in (c1 + 1):length(ccodes)) {
        pt1 <- suppressMessages(sf::st_coordinates(cshp.simple[c1,])[,c("X", "Y")])
        pt2 <- suppressMessages(sf::st_coordinates(cshp.simple[c2,])[,c("X", "Y")])
        dist <- suppressMessages(min(sp::spDists(pt1, pt2, longlat=TRUE)))
        resultmatrix[c1, c2] <- dist
        resultmatrix[c2, c1] <- dist
      }
    }

    ## Capital distance
  } else if (type == "capdist"){
    pts <- as.matrix(sf::st_drop_geometry(cshp.part)[,c("caplong", "caplat")])
    resultmatrix <- sp::spDists(pts, pts, longlat=TRUE)

    ## Centroid distance
  } else {
    pts <- as.matrix(sf::st_coordinates(suppressWarnings(sf::st_centroid(cshp.part))))
    resultmatrix <- sp::spDists(pts, pts, longlat=TRUE)
  }

  ## Assign country codes to matrix rows and columns
  colnames(resultmatrix) <- ccodes
  rownames(resultmatrix) <- ccodes
  return(resultmatrix)
}

