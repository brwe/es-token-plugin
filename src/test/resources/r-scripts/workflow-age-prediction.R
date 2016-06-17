library(pmml)
library(pmmlTransformations)
library(caret)

library(graphics)
library(Metrics)
library(rpart)

# read and clean data
mydata <- read.csv("/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/adult.data", stringsAsFactors=FALSE, na.strings = c("")) #, check.names=FALSE)
mydata$workclass<-replace(mydata$workclass,which(is.na(mydata$workclass)), rep("too-cool-to-work", sum(is.na(mydata$workclass))))
mydata$occupation<-replace(mydata$occupation,which(is.na(mydata$occupation)), rep("hedonist", sum(is.na(mydata$occupation))))
mydata$native_country<-replace(mydata$native_country,which(is.na(mydata$native_country)), rep("Fiji", sum(is.na(mydata$native_country))))
mydata$native_country[which(mydata$native_country == "Holand-Netherlands")]<-"England"
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



# split into test and train set
intrain<-createDataPartition(y=mydata$class,p=0.5,list=FALSE)
training<-mydata[intrain,]

testing<-mydata[-intrain,]
levels(training$native_country)<- levels(mydata$native_country)
levels(testing$native_country)<- levels(mydata$native_country)
# train model
mylogit <- glm(age ~ hours_per_week + workclass + education  + marital_status + occupation + relationship + race + sex +native_country + hours_per_week , 
               data = training)

prob <-predict(mylogit, newdata = testing, type = "response")
mse(prob, testing$age)
