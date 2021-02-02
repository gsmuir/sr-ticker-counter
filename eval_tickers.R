### AUTHORS ###################################################################
# Name      Date
# Gareth    Wed Jan 27 16:08:07 2021
#

### DESCRIPTION ###############################################################
#  

### LIBRARIES #################################################################
library(rhdf5)
library(anytime)
library(plyr)
library(quantmod)

### FUNCTIONS #################################################################
get.timepoints <- function(h5file) {
  
  #€ get timepoints of the data
  tp <- h5ls(h5file)[h5ls(h5file)$otype == "H5I_GROUP", "name"]
  tp <- tp[grepl("_", tp)]
  dates_format = as.Date(gsub("_", " ", tp), format = "%B %d %Y")
  
  ts.data <- data.frame(
    ID = tp,
    Date = dates_format,
    stringsAsFactors = F
  );
  return(ts.data)
} 

read.timepoint <- function(h5file, TID) {
  
  # read in timepoint
  tkr_cnt <- h5read(
    h5file, 
    name = paste0(TID, "/Count")
  );
  
  tkr <- h5read(
    h5file, 
    name = paste0(TID, "/Tickers")
  );
  
  timepoint.tkr.dat <- data.frame(t(setNames(tkr_cnt, tkr)))
  rownames(timepoint.tkr.dat) <- TID
  
  return(timepoint.tkr.dat)
}

# merge the time points
merge.timepoints <- function(x) {
  tickers.merged <- rbind.fill(x)
  tickers.merged[is.na(tickers.merged)] <- 0
  tickers.merged <- t(tickers.merged)
  colnames(tickers.merged) <- sapply(x, function(y) rownames(y))
  return(tickers.merged)
}

### MAIN ######################################################################
h5file <- "out/out_tickers_stocks.h5"
time.points <- get.timepoints(h5file)
min.date = min(time.points$Date)
max.date = max(time.points$Date)
num.timepoints = nrow(time.points)

# read in all time points
tps.ticker.data <- lapply(time.points$ID, function(x) read.timepoint(h5file, x))

# merge into matrixx  
merged.timepoints <- merge.timepoints(tps.ticker.data)
merged.timepoints <- merged.timepoints[rowSums(merged.timepoints) > 5,]

# get the ranksum of each ticker and order
merged.timepoints.ranks <- sapply(1:ncol(merged.timepoints), function(x) rank(desc(merged.timepoints[,x])))
dimnames(merged.timepoints.ranks) <- dimnames(merged.timepoints)
plot(ticker.day.ranks['GME',])
merged.timepoints['GM',]
merged.timepoints.ranks['GME',]

# ranksum
ticker.ranks <- sort(
  setNames(rowSums(merged.timepoints.ranks), rownames(merged.timepoints)), decreasing = F
  );

ticker.day.ranks['PLTR',]

merged.timepoints.ranks.sorted <- merged.timepoints.ranks[names(ticker.ranks),]
merged.timepoints.ranks.sorted[1:15,]


plot( 1:num.timepoints, merged.timepoints.ranks.sorted['GME', ])

# TODO ticker prices
getSymbols(
  "GME", 
  from = min.date,
  to = max.date, 
  warnings = FALSE,
  auto.assign = TRUE
  );
