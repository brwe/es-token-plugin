library(pmml)
library(pmmlTransformations)
library(caret)

library(graphics)

library(rpart)
library(e1071)
library(nnet)
library(ada)

script.dir <- getSrcDirectory(function(x) {
  x
})
rmlist = (ls())
script <- paste(script.dir, "/dataHelperFunctions.R", sep = "")
result.dir <-
  paste(script.dir, "/../org/elasticsearch/script/", sep = "")
source(script)


# try naive bayes
# load data and replace missing values
data <- prepareData()

intrain <-
  createDataPartition(y = data$class, p = 0.5, list = FALSE)
training <- data[intrain, ]
testing <- data[-intrain, ]

myNB <- naiveBayes(
  class ~ age + workclass + education_num
  + marital_status + occupation + relationship
  + race + sex + hours_per_week
  + native_country + capital_loss + capital_gain,
  data = training
)
result  <- predict(myNB, testing)
result <- data.frame(result)

cMatrixNB <-
  confusionMatrix(
    result$result,
    testing$class,
    positive = NULL,
    dnn = c("result", "class")
  )

pmmlModel <- pmml(myNB, predictedField = "class")
write(toString.XMLNode(pmmlModel), 
      file = "/Users/britta/poc/example-elasticsearch/elasticsearch-2.2.1/config/scripts/naive_bayes.pmml_model")

# train logistic regression model
mylogit <- glm(
  class ~ age + workclass + education 
  + marital_status + occupation + relationship
  + race + sex + hours_per_week
  + native_country + capital_loss + capital_gain,
  data = training,
  family = "binomial"
)

prob <- predict(mylogit, newdata = testing, type = "response")
result <- sapply(prob, function(x)
  if (x > 0.5) {
    ">50K"
  } else{
    "<=50K"
  })
result <- data.frame(result)
cMatrixLR <-
  confusionMatrix(
    result$result,
    testing$class,
    positive = NULL,
    dnn = c("result", "class")
  )
pmmlModel <- pmml(mylogit)
write(toString.XMLNode(pmmlModel), file = "/Users/britta/poc/example-elasticsearch/elasticsearch-2.2.1/config/scripts/logistic.pmml_model")

# don't want to spend more time on it

# try neural network
myNN <-
  nnet(
    class ~ age + workclass + education + occupation + race + sex + hours_per_week + marital_status + relationship
    + native_country,
    size = 10,
    data = training,
    maxit = 1000
  )
result  <- predict(myNN, testing)
result <- sapply(result, function(x)
  if (x > 0.5) {
    ">50K"
  } else{
    "<=50K"
  })
result <- data.frame(result)
cMatrixNN <-
  confusionMatrix(
    result$result,
    testing$class,
    positive = NULL,
    dnn = c("result", "class")
  )


# train tree model
myTree <- rpart(
  class ~ age + workclass + education + education_num
  + marital_status + occupation + relationship
  + race + sex + hours_per_week
  + native_country + capital_loss + capital_gain,
  data = training,
  na.action = na.rpart
)
result <- predict(myTree, newdata = testing, type = "class")
result <- data.frame(result)
cMatrixTree <-
  confusionMatrix(
    result$result,
    testing$class,
    positive = NULL,
    dnn = c("result", "class")
  )
pmmlModel<-pmml(myTree)
write(toString.XMLNode(pmmlModel), file = "/Users/britta/poc/example-elasticsearch/elasticsearch-2.2.1/config/scripts/tree.pmml_model")

# try with boosting
training <- data[intrain,]
testing <- data[-intrain,]
mydata2 <- training[,!(names(training) %in% c("class"))]
myAda <- ada(mydata2, training$class)
result  <- predict(myAda, testing)
result <- data.frame(result)
cMatrixAda <-
  confusionMatrix(
    result$result,
    testing$class,
    positive = NULL,
    dnn = c("result", "class")
  )


results <-
  data.frame(
    cbind(
      cMatrixNB$byClass,
      cMatrixLR$byClass,
      cMatrixNN$byClass,
      cMatrixTree$byClass,
      cMatrixAda$byClass
    )
  )
colnames(results) <-
  c("Naive Bayes",
    "Logistic Regression",
    "Neural Network",
    "Tree",
    "Ada")

# try svm
# or maybe not because it takes forever...
# mySVM <- svm(class ~ age + workclass + education + education_num
#              + marital_status + occupation + relationship
#              + race + sex + hours_per_week
#              + native_country + capital_loss + capital_gain
#              , data = training, cost = 100, gamma = 1)
# result  <- predict(mySVM, testing)
# result<-data.frame(result)
#
# confusionMatrix(result$result, testing$class, positive = NULL,
#                                 dnn = c("result", "class"))
