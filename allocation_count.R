source("spark_submit.R")

step <- function(jar, interval, timeslots, redis) {
  args <- c("--interval", as.integer(interval), 
            "--timeSlots", timeslots, 
            "--redisHost", redis$host, 
            "--redisPort", as.integer(redis$port))
  
  spec(
    name = sprintf("Allocation Count - %s", timeslots), 
    args = args, 
    jar = jar, 
    main = "com.valuepotion.advertising.allocation.applications.AllocationCount",
    action_on_failure = "CANCEL_AND_WAIT"
  )
}

run <- function(cluster, jar, interval = 300000L, timeslots, redis) {
  submit(cluster, step(jar, interval, timeslots, redis))
}

create_cluster_and_run <- function(
  jar,
  interval = 300000L,
  timeslots,
  redis,
  access_key_name,  
  master = list(count = 1, type = "m3.xlarge"), 
  core = list(count = 3, type = "m3.xlarge"), 
  log_uri = NA, 
  auto_terminate = TRUE) {
 
  create_cluster(
    name = "Allocation Count App Cluster", 
    specs = step(jar, interval, timeslots, redis),
    access_key_name = "aws-singapore",
    auto_terminate = TRUE)
}
