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
  
  cmd <- "aws"
  args <- c("emr", "add-steps", "--cluster-id", cluster, "--steps", sprintf("file://%s", f))
  status <- system2(cmd, args) 
  if (status != 0) {
    stop(sprintf("Failed to add steps: %s %s", cmd, paste(args, collapse = " ")))
  }
}
