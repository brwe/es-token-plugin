library(pmml)
library(pmmlTransformations)


rmlist=(ls())
source("/home/britta/es-token-plugin/src/test/resources/r-scripts/dataHelperFunctions.R")

mydata<-prepareData()
# pre processing
mydataWrapped<-WrapData(mydata)
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="age->age_z")
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="education_num->education_num_z")
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="hours_per_week->hours_per_week_z")

# train model
mylogit <- glm(class ~ age_z + workclass + education + education_num_z + marital_status + occupation + relationship + race + sex + hours_per_week_z + native_country , 
               data = mydataWrapped$data, family = "binomial", na.action = na.pass)

# convert to pmmlgetwd
pmmlModel <- pmml(mylogit, transform=mydataWrapped)


attributes <- data.frame(c("too-cool-to-work"),c("hedonist"),c("Fiji"))
rownames(attributes) <- c("missingValueReplacement")
colnames(attributes) <- c("workclass", "occupation","native_country")
pmmlModel <- addMSAttributes(pmmlModel, attributes=attributes)
write(toString.XMLNode(pmmlModel), file = "/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/glm-adult-full-r.xml")

prob <-predict(mylogit, newdata = mydataWrapped$data, type = "response")
result<-sapply(prob, function(x)if(x>0.5){">50K"}else{"<=50K"})
compresult = data.frame(sapply(prob,function(x){1.0-x}),prob,  result)
colnames(compresult)<-c("probClass0", "probClass1" ,"predictedClass")

write.table(compresult, file="/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/r_glm_adult_result.csv",row.names = F, sep=",")
