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

library(FC14.plotting.lib)
library(quantmod)
library(latticeExtra)

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
  
  # order by date
  ts.data <- ts.data[ order(ts.data$Date, decreasing = F) ,]
  ts.data$DateID <- make.names(ts.data$Date)
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


to.strdate <- function(x) {
  time.points[match(x, time.points$DateID),"ID"]
}

get.merged.data <- function(ticker.i, transform = "none"){
  stock.price <- get.stock.price(ticker.i,  min.date, max.date)
  stock.count <- get.ticker.count(ticker.i) # min max date for stock counts
  
  merged.count <- as.data.frame(
    cbind(Count = stock.count, stock.price[match(names(stock.count), rownames(stock.price)),])
  );
  
  merged.count$Ticker = ticker.i
  merged.count$Date = rownames(merged.count)
  colnames(merged.count) <- gsub(paste0(ticker.i,"."), "", colnames(merged.count))
  
  # transform 
  if (transform == "log") {
    merged.count$Count <- log2(merged.count$Count + 1)
  } else if (transform == "sqrt") {
    merged.count$Count <- sqrt(merged.count$Count)
  } else if (transform == "rollMean") {
    merged.count$Count <- rollmean(log2(merged.count$Count + 1), k = 3, fill = NA)
  }
  merged.count$Transform = transform
  return(merged.count)
}

get.ticker.count <-  function(ticker.i) {
  stock.count <- merged.timepoints[ticker.i,]
  return(stock.count)
}

get.stock.price <- function(stock, from.date, to.date) {
  
  # TODO ticker prices
  soi <- as.data.frame(
    getSymbols(
      stock, 
      from = from.date,
      to = to.date, 
      warnings = FALSE,
      auto.assign = F
    ))
  
  tmp.date <- rownames(soi)
  tmp.DateID <- make.names(tmp.date)
  
  rownames(soi) <- time.points[match( tmp.DateID, time.points$DateID),'ID']
  soi$ID  == time.points[match( tmp.DateID, time.points$DateID),'ID']  
  
  return(soi)
}

### MAIN ######################################################################

h5file <- "out/out_tickers_wallstreetbets.h5"
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
#merged.timepoints.ranks <- sapply(1:ncol(merged.timepoints), function(x) rank(desc(merged.timepoints[,x])))
#dimnames(merged.timepoints.ranks) <- dimnames(merged.timepoints)



merge.plot <- function(dat){
  
  # x axis scale 
  dat$dateNumeric <- 1:nrow(dat)
  
  if (unique(dat$Transform) == "log") {
    yax <- expression(paste('Count', ' (log'[2],")" ))
  } else if (unique(dat$Transform) == "sqrt") {
    yax <- unique(dat$Transform) 
  } else if (unique(dat$Transform) == "rollMean") {
    yax <- "rolling avg (k=3)"
  }

  cnt.plot <- xyplot(
    Count ~ dateNumeric, dat, type = "l" , lwd = 3,
    ylab = list(yax, fontsize = 12),
    ylab.right = list("Price ($)", fontsize = 12),
    main = unique(dat$Ticker))
  
  stock.plot <- xyplot(Adjusted ~ dateNumeric, dat, type = "s", lwd = 3 )
  doubleYScale(cnt.plot, stock.plot)
}


ticker.i <- "TSLA"
select.ticker.dat <- get.merged.data(ticker.i, transform = "rollMean")
merge.plot(select.ticker.dat)



select.ticker.dat <- get.merged.data(ticker.i, transform = "log")
merge.plot(select.ticker.dat)




