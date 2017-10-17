#!/usr/bin/Rscript --vanilla


basedir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  m <- grep(needle, args)
  if (length(m) > 0) {
    f <- normalizePath(sub(needle, "", args[m]))
  } else {
    f <- normalizePath(sys.frames()[[1]]$ofile)
  }
  
  dirname(f)
}

arg <- function(name) {
  args <- commandArgs(TRUE)
  
  t0 <- NA
  m <- match(sprintf("--%s", name), args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }
  
  t0
}


setwd(basedir())
source("spark_submit.R")

TIMESLOT_LENGTH <- 300000L
TIMESLOT_LENGTH_IN_SECONDS <- TIMESLOT_LENGTH / 1000L
timeslot <- function(sl) {
  ct <-as.POSIXct(Sys.time(), tz = "UTC")
  format(
    as.POSIXct(floor((as.double(ct) - sl) / sl) * sl, tz = "UTC", origin = "1970-01-01"),
    format ="%Y%m%d%H%M",
    tz = "UTC")
}

run <- function(cluster, jar, interval = TIMESLOT_LENGTH, timeslot, redis, service, propagation) {
  args <- c("--interval", as.integer(interval), 
            "--timeSlot", timeslot, 
            "--redis.host", redis$host, 
            "--redis.port", as.integer(redis$port),
            "--service", service,
			"--propagation.api", propagation$api,
			"--propagation.key", propagation$key) 

  s <- spec(
    name = sprintf("Allocation Planning - %s", timeslot), 
    args = args, 
    jar = jar, 
    main = "com.valuepotion.advertising.allocation.applications.Planning",
    action_on_failure = "CONTINUE"
  )
  
  submit(cluster, s)
}

ts <- arg("timeSlot")
ts <- if (is.na(ts)) timeslot(TIMESLOT_LENGTH_IN_SECONDS) else ts

run(cluster = arg("cluster"),
    jar = arg("jar"),
    timeslot = ts,
    redis = list(host = arg("redis.host"), port = as.integer(arg("redis.port"))),
	service = arg("service"),
	propagation = list(api = arg("propagation.api"), key = arg("propagation.key")))
