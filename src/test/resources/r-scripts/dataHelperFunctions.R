

loadData <- function() {
  script.dir <- getSrcDirectory(function(x) {
    x
  })
  rmlist = (ls())
  adult <-
    paste(script.dir,
          "/../org/elasticsearch/script/adult.data",
          sep = "")
  data <-
    read.csv(
      adult,
      header = TRUE,
      stringsAsFactors = FALSE,
      na.strings = c("")
    )
  return(data)
}

replaceMissingValues <- function(data) {
  data$workclass <-
    replace(data$workclass,
            which(is.na(data$workclass)),
            rep("too-cool-to-work", sum(is.na(data$workclass))))
  data$occupation <-
    replace(data$occupation, which(is.na(data$occupation)), rep("hedonist", sum(is.na(data$occupation))))
  data$native_country <-
    replace(data$native_country, which(is.na(data$native_country)), rep("Fiji", sum(is.na(
      data$native_country
    ))))
  return(data)
}

convertStringsToFactors <- function(data) {
  data$class <- factor(data$class)
  data$education <- factor(data$education)
  data$marital_status <- factor(data$marital_status)
  data$occupation <- factor(data$occupation)
  data$relationship <- factor(data$relationship)
  data$race <- factor(data$race)
  data$sex <- factor(data$sex)
  data$native_country <- factor(data$native_country)
  data$workclass <- factor(data$workclass)
  return(data)
}

prepareData <- function() {
  data <- loadData()
  data <- replaceMissingValues(data)
  data <- convertStringsToFactors(data)
  return(data)
}

normalizeNumberVariables <- function(data) {
  data$age <- scale(data$age)
  data$fnlwgt <- scale(data$fnlwgt)
  data$capital_gain <- scale(data$capital_gain)
  data$capital_loss <- scale(data$capital_loss)
  data$hours_per_week <- scale(data$hours_per_week)
  return(data)
}