library(pmml)
library(pmmlTransformations)

mydata <- read.csv("/Users/britta/es-token/src/test/resources/org/elasticsearch/script/adult.data", stringsAsFactors=FALSE, na.strings = c("", " "))

# replace values
mydata$workclass<-replace(mydata$workclass,which(is.na(mydata$workclass)), rep("hedonist", sum(is.na(mydata$workclass))))
# make factiors
mydata$workclass <- factor(mydata$workclass)
mydata$class <- factor(mydata$class)
mydataWrapped<-WrapData(mydata)

# mydataWrapped <- FunctionXform(mydataWrapped,origFieldName="workclass", newFieldName="workclass*",mapMissingTo = "blah")
mydataWrapped<-WrapData(mydata)
mydataWrapped <- RenameVar(mydataWrapped,"workclass->workclass_mr",mapMissingTo="hedonist")

# train model
mylogit <- glm(class ~ age + workclass + fnlwgt, data = mydata$data, family = "binomial")

# convert to pmml
pmml(mylogit, transform=mydataWrapped)

https://elasticsearch.zendesk.com/agent/tickets/25752