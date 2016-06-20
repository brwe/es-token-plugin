library(pmml)
library(pmmlTransformations)
library(pmml)
library(pmmlTransformations)
library(caret)
library(graphics)
library(rpart)
library(e1071)
library(nnet)

rmlist=(ls())
script.dir <- getSrcDirectory(function(x) {x})
rmlist=(ls())
script <- paste(script.dir, "/dataHelperFunctions.R", sep="")
result.dir<-paste(script.dir, "/../org/elasticsearch/script/", sep="")
source(script)

data<- prepareData();


myNB <- naiveBayes(class ~ age + workclass + education_num + marital_status + occupation + relationship + race + sex + hours_per_week 
                   + native_country, data = data)

# convert to pmmlgetwd
pmmlModel <- pmml(myNB, predictedField = "class")

attributes <- data.frame(c("too-cool-to-work"),c("hedonist"),c("Fiji"))
rownames(attributes) <- c("missingValueReplacement")
colnames(attributes) <- c("workclass", "occupation","native_country")
pmmlModel <- addMSAttributes(pmmlModel, attributes=attributes)
write(toString.XMLNode(pmmlModel), file = paste(result.dir, "naive-bayes-adult-full-r.xml", sep=""))

prediction  <- predict(myNB, data, type = "raw")

predictedClasses  <- predict(myNB, data, type = "class")
result<-data.frame(prediction)
result$class<-predictedClasses
colnames(result)<-c("probClass0", "probClass1" ,"predictedClass")
cMatrix <-confusionMatrix(result$predictedClass, data$class, positive = NULL, 
                          dnn = c("result", "class"))

write.table(result, file = paste(result.dir, "r_naive_bayes_adult_result.csv", sep =""),row.names = F, sep=",")
