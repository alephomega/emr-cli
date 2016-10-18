library(jsonlite)

spec <- function(name, args = character(0), jar, main, action_on_failure = c("CONTINUE", "TERMINATE_CLUSTER", "CANCEL_AND_WAIT")) {
  toJSON(
    list(
      Name = name,
      Args = c(c("--deploy-mode", "cluster", "--class", main, jar), args),
      Type = "SPARK",
      ActionOnFailure = action_on_failure[1]
    ),
    auto_unbox = TRUE
  )
}

submit <- function(cluster, specs) {
  f <- tempfile(pattern = "step.", tmpdir = tempdir(), fileext = ".json")
  file.create(f, recursive = TRUE)
  cat(sprintf("[%s]", paste(specs, collapse = ",")), file = f, append = FALSE)
  
  on.exit(if (file.exists(f)) unlink(f))
  
  cmd <- "aws"
  args <- c("emr", "add-steps", "--cluster-id", cluster, "--steps", sprintf("file://%s", f))
  status <- system2(cmd, args) 
  if (status != 0) {
    stop(sprintf("Failed to add steps: %s %s", cmd, paste(args, collapse = " ")))
  }
}

create_cluster <- function(
  name, 
  release_label = "emr-5.0.0", 
  master = list(count = 1, type = "m3.xlarge"), 
  core = list(count = 3, type = "m3.xlarge"), 
  specs, 
  access_key_name, 
  auto_terminate = TRUE, 
  enable_debugging = FALSE, 
  log_uri = NA,
  properties = list(maximizeResourceAllocation = "true")) {

  applications <- list(
    list(Name = "Spark")
  )
  
  instance_groups <- list(
    list(
      InstanceGroupType = "MASTER",
      InstanceCount = master$count,
      InstanceType = master$type
    ),
    
    list(
      InstanceGroupType = "CORE",
      InstanceCount = core$count,
      InstanceType = core$type
    )
  )
  
  ec2_attributes <- list(
    KeyName = access_key_name
  )
  
  configurations <- list(
    list(
      Classification = "spark",
      Properties = properties
    )
  )
  
  steps_json <- tempfile(pattern = "step.", tmpdir = tempdir(), fileext = ".json")
  file.create(steps_json, recursive = TRUE)
  cat(sprintf("[%s]", paste(specs, collapse = ",")), file = steps_json, append = FALSE)
  
  on.exit(if (file.exists(steps_json)) unlink(steps_json))
  
  cmd <- "aws"
  args <- c("emr", "create-cluster", 
            "--steps", sprintf("file://%s", steps_json),
            "--release-label", release_label,
            "--name", sprintf("'%s'", name),
            "--applications", sprintf("'%s'", toJSON(applications, auto_unbox = TRUE)), 
            "--ec2-attributes", sprintf("'%s'", toJSON(ec2_attributes, auto_unbox = TRUE)),
            "--configurations", sprintf("'%s'", toJSON(configurations, auto_unbox = TRUE)),
            "--instance-groups", sprintf("'%s'", toJSON(instance_groups, auto_unbox = TRUE)),
            "--use-default-roles")

  if (enable_debugging) {
    args <- c(args, "--enable-debugging")  
  }
  
  if (!is.na(log_uri)) {
    args <- c(args, c("--log-uri", log_uri))
  }
  
  if (auto_terminate) {
    args <- c(args, "--auto-terminate")
  }
  
  status <- system2(cmd, args) 
  if (status != 0) {
    stop(sprintf("Failed to add steps: %s %s", cmd, paste(args, collapse = " ")))
  }
}
