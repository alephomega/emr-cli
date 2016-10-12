source("spark_submit.R")

run <- function(cluster, jar, interval = 30000L, timeslot, redis) {
  args <- c("--interval", as.integer(interval), "--timeSlot", timeslot, "--redisHost", redis$host, "--redisPort", as.integer(redis$port))
  s <- spec(
    name = sprintf("Allocation Planning - %s", timeslot), 
    args = args, 
    jar = jar, 
    main = "com.valuepotion.advertising.allocation.applications.Planning",
    action_on_failure = "CONTINUE"
  )
  
  submit(cluster, s)
}
