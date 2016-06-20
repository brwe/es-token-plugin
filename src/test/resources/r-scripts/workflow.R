library(pmml)
library(pmmlTransformations)
library(caret)

library(graphics)

library(rpart)
library(e1071)
library(nnet)


source("/home/britta/es-token-plugin/src/test/resources/r-scripts/dataHelperFunctions.R")

data<-prepareData()

# split into test and train set
intrain<-createDataPartition(y=data$class,p=0.5,list=FALSE)
training<-data[intrain,]

testing<-data[-intrain,]

# train model
mylogit <- glm(class ~ age + hours_per_week + workclass + education + education_num + marital_status + occupation + relationship + race + sex +native_country + hours_per_week , 
               data = training, family = "binomial")

prob <-predict(mylogit, newdata = testing, type = "response")
result<-sapply(prob, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))

# find we can remove education_num

mylogit <- glm(class ~ age + hours_per_week + workclass + education  + marital_status + occupation + relationship + race + sex +native_country + hours_per_week , 
               data = training, family = "binomial")

prob <-predict(mylogit, newdata = testing, type = "response")
result<-sapply(prob, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))


# train tree model
myTree <- rpart(class ~ age + workclass + education + education_num + marital_status + occupation + relationship + race + sex + hours_per_week + native_country , 
                data = training, na.action = na.rpart)
result <-predict(myTree, newdata = testing, type = "class")
result<-data.frame(result)
confusionMatrix(result$result, testing$class, positive = NULL, 
                dnn = c("result", "class"))


# try svm
mySVM <- svm(class ~ age + workclass + education + education_num + marital_status + occupation + relationship + race + sex + hours_per_week + native_country, data = training, cost = 100, gamma = 1)
result  <- predict(mySVM, testing)
result<-data.frame(result)

confusionMatrix(result$result, testing$class, positive = NULL, 
                                dnn = c("result", "class"))

# that did not go well...


intrain<-createDataPartition(y=data$class,p=0.5,list=FALSE)
training<-data[intrain,]

testing<-data[-intrain,]
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



# try naive bayes

data<- prepareData();

intrain<-createDataPartition(y=data$class,p=0.5,list=FALSE)
training<-data[intrain,]
testing<-data[-intrain,]

myNB <- naiveBayes(class ~ age + workclass + education_num + marital_status + occupation + relationship + race + sex + hours_per_week 
             + native_country, data = training)
result  <- predict(myNB, testing)
result<-data.frame(result)

cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                          dnn = c("result", "class"))

# Neural Network


# read and clean data
data<- prepareData();

intrain<-createDataPartition(y=data$class,p=0.5,list=FALSE)
training<-data[intrain,]

testing<-data[-intrain,]
myNN <- nnet(class ~ age + workclass + education + occupation + race + sex + hours_per_week + marital_status + relationship 
             + native_country, size=10, data=training,maxit=1000)
result  <- predict(myNN, testing)
result<-sapply(result, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                          dnn = c("result", "class"))

data<-normalizeNumberVariables(data)
intrain<-createDataPartition(y=data$class,p=0.5,list=FALSE)

training<-data[intrain,]

testing<-data[-intrain,]


myNN <- nnet(class ~ age + workclass + education + occupation + race + sex + hours_per_week + marital_status + relationship 
             + native_country, size=10, data=training,maxit=1000)
result  <- predict(myNN, testing)
result<-sapply(result, function(x)if(x>0.5){">50K"}else{"<=50K"})
result<-data.frame(result)
cMatrix <-confusionMatrix(result$result, testing$class, positive = NULL, 
                          dnn = c("result", "class"))


