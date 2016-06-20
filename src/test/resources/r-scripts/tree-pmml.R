library(pmml)
library(pmmlTransformations)
library(rpart)


rmlist=(ls())
source("/home/britta/es-token-plugin/src/test/resources/r-scripts/dataHelperFunctions.R")

mydata<-prepareData()
# pre processing
mydataWrapped<-WrapData(mydata)
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="age->age_z")
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="education_num->education_num_z")
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="hours_per_week->hours_per_week_z")

# train model
myTree <- rpart(class ~ age_z + workclass + education + education_num_z + marital_status + occupation + relationship + race + sex + hours_per_week_z + native_country , 
               data = mydataWrapped$data, na.action = na.rpart)

# convert to pmmlgetwd
pmmlModel <- pmml(myTree, transform=mydataWrapped)

attributes <- data.frame(c("too-cool-to-work"),c("hedonist"),c("Fiji"))
rownames(attributes) <- c("missingValueReplacement")
colnames(attributes) <- c("workclass", "occupation","native_country")
pmmlModel <- addMSAttributes(pmmlModel, attributes=attributes)
write(toString.XMLNode(pmmlModel), file = "/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/tree-adult-full-r.xml")

prob <-predict(myTree, newdata = mydataWrapped$data, type = "class")
compresult = data.frame(prob)
colnames(compresult)<-c("predictedClass")

write.table(compresult, file="/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/r_tree_adult_result.csv",row.names = F)
