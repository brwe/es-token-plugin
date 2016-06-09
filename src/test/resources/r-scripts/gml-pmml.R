library(pmml)
library(pmmlTransformations)

# read and clean data
mydata <- read.csv("/Users/britta/es-token/src/test/resources/org/elasticsearch/script/adult.data", stringsAsFactors=FALSE, na.strings = c("", " "))
mydata$workclass<-gsub(" ", "", mydata$workclass, fixed = TRUE)

# make class a factor
mydata$class <- factor(mydata$class)

# pre processing
mydataWrapped<-WrapData(mydata)
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="age->age_z",mapMissingTo=1)
# this is annoying, but I did not find out how to just replace missing in one transform.
mydataWrapped <- MapXform(mydataWrapped, xformInfo="[workclass -> workclass_mr][string->string]",
                          table="/Users/britta/es-token/src/test/resources/r-scripts/mapworkclass.csv",mapMissingTo="hedonist")
# make workclass_mr a factor
mydataWrapped$data$workclass_mr <- factor(mydataWrapped$data$workclass_mr)

# train model
mylogit <- glm(class ~ age_z + workclass_mr + fnlwgt, data = mydataWrapped$data, family = "binomial", na.action = na.pass)

# convert to pmml
pmml(mylogit, transform=mydataWrapped)
