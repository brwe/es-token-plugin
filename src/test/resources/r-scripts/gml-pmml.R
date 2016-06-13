library(pmml)
library(pmmlTransformations)

# read and clean data
mydata <- read.csv("/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/adult.data", stringsAsFactors=FALSE, na.strings = c("")) #, check.names=FALSE)
mydata$workclass<-replace(mydata$workclass,which(is.na(mydata$workclass)), rep("too-cool-to-work", sum(is.na(mydata$workclass))))
mydata$occupation<-replace(mydata$occupation,which(is.na(mydata$occupation)), rep("hedonist", sum(is.na(mydata$occupation))))
mydata$native_country<-replace(mydata$native_country,which(is.na(mydata$native_country)), rep("Fiji", sum(is.na(mydata$native_country))))

# make class a factor
mydata$class <- factor(mydata$class)
mydata$education <- factor(mydata$education)
mydata$marital_status <- factor(mydata$marital_status)
mydata$occupation <- factor(mydata$occupation)
mydata$relationship <- factor(mydata$relationship)
mydata$race <- factor(mydata$race)
mydata$sex <- factor(mydata$sex)
mydata$native_country <- factor(mydata$native_country)
mydata$workclass <- factor(mydata$workclass)

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

