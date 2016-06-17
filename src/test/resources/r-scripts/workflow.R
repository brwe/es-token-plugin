library(pmml)
library(pmmlTransformations)
library(caret)

library(graphics)

library(rpart)
library(e1071)
library(nnet)
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
mylogit <- glm(class ~ age + hours_per_week + workclass + education + education_num + marital_status + occupation + relationship + race + sex +native_country + hours_per_week , 
               data = training, family = "binomial")

prob <-predict(mylogit, newdata = testing, type = "response")
result<-sapply(prob, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))

mylogit
# find we can remove education_num

mylogit <- glm(class ~ age + hours_per_week + workclass + education  + marital_status + occupation + relationship + race + sex +native_country + hours_per_week , 
               data = training, family = "binomial")

prob <-predict(mylogit, newdata = testing, type = "response")
result<-sapply(prob, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))


# train model
myTree <- rpart(class ~ age + workclass + education + education_num + marital_status + occupation + relationship + race + sex + hours_per_week + native_country , 
                data = mydata, na.action = na.rpart)
result <-predict(myTree, newdata = testing, type = "class")
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))


mySVM <- svm(class ~ age + workclass + education + education_num + marital_status + occupation + relationship + race + sex + hours_per_week + native_country, data = training, cost = 100, gamma = 1)
result  <- predict(mySVM, testing)
result<-data.frame(result)

confusionMatrix(result$result, testing$class, positive = NULL, 
                                dnn = c("result", "class"))

# that did not go well...

mydata$age<-scale(mydata$age)
mydata$fnlwgt<-scale(mydata$fnlwgt)
mydata$capital_gain<-scale(mydata$capital_gain)
mydata$capital_loss<-scale(mydata$capital_loss)
mydata$hours_per_week<-scale(mydata$hours_per_week)



intrain<-createDataPartition(y=mydata$class,p=0.5,list=FALSE)
training<-mydata[intrain,]

testing<-mydata[-intrain,]
accuracies <- vector(mode="double", length=20)

gamma<-0.0001

for(i in c(1:4)) {
  cparam<-1
  for(j in c(1:4)){
    print(paste("starting with gamma ", gamma, " and C ", cparam, sep=" "))
    mySVM <- svm(class ~ age + workclass + education_num + marital_status + occupation + relationship + race + sex + hours_per_week 
             + native_country, data = training, cost = cparam, gamma = gamma)
    result  <- predict(mySVM, testing)
    result<-data.frame(result)

    cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))

    accuracies[i]<-cMatrix$overall["Accuracy"]

    cparam=cparam*10
    print(paste("done with gamma ", gamma, " and C, accuracy is ", accuracies[i], cparam, sep=" "))
  }
  gamma=gamma*10
}
# that is better but still not super...
print(accuracies)

myNB <- naiveBayes(class ~ age + workclass + education_num + marital_status + occupation + relationship + race + sex + hours_per_week 
             + native_country, data = training)
result  <- predict(myNB, testing)
result<-data.frame(result)

cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                          dnn = c("result", "class"))


myMultinNom <- multinom(class ~ age + workclass + education_num + marital_status + occupation + relationship + race + sex + hours_per_week 
                        + native_country, data = training)











# Neural Network


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


myNN <- nnet(class ~ age + workclass + education + occupation + race + sex + hours_per_week + marital_status + relationship 
             + native_country, size=10, data=training,maxit=1000)
result  <- predict(myNN, testing)
result<-sapply(result, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                                                    dnn = c("result", "class"))
