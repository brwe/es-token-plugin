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

result.dir<-paste(script.dir, "/../org/elasticsearch/script/", sep="")
print(script.dir)
script <- paste(script.dir, "/dataHelperFunctions.R", sep="")
source(script)

data<- loadData()
data<-convertStringsToFactors(data)
threshold = 1.0/32560.0
myNB <- naiveBayes(class ~ age + fnlwgt+education + education_num + marital_status  + relationship + race + sex + hours_per_week 
                   + capital_gain + capital_loss, data = data, threshold = 3.071253e-05) #threshold = 1.0/32560.0

# convert to pmmlgetwd
pmmlModel <- pmml(myNB, predictedField = "class")

write(toString.XMLNode(pmmlModel), file = paste(result.dir, "naive-bayes-adult-full-r-no-missing-values.xml", sep=""))

prediction  <- predict(myNB, data, type = "raw")

predictedClasses  <- predict(myNB, data, type = "class")
result<-data.frame(prediction)
result$class<-predictedClasses
colnames(result)<-c("probClass0", "probClass1" ,"predictedClass")
cMatrix <-confusionMatrix(result$predictedClass, data$class, positive = NULL, 
                          dnn = c("result", "class"))

write.table(result, file = paste(result.dir, "r_naive_bayes_adult_result_no_missing_values.csv", sep =""),row.names = F, sep=",")
