library(pmml)
library(pmmlTransformations)
library(rpart)


rmlist=(ls())
script.dir <- getSrcDirectory(function(x) {x})
rmlist=(ls())
script <- paste(script.dir, "/dataHelperFunctions.R", sep="")
result.dir<-paste(script.dir, "/../org/elasticsearch/script/", sep="")
source(script)

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
write(toString.XMLNode(pmmlModel), file = paste(result.dir, "tree-adult-full-r.xml", sep=""))

prob <-predict(myTree, newdata = mydataWrapped$data, type = "class")
compresult = data.frame(prob)
colnames(compresult)<-c("predictedClass")

write.table(compresult, file=paste(result.dir, "r_tree_adult_result.csv", sep=""),row.names = F)
