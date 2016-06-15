library(pmml)
library(pmmlTransformations)
library(rpart)
# read and clean data
mydata <- read.csv("/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/adult.data", stringsAsFactors=FALSE, na.strings = c("")) #, check.names=FALSE)
mydata$work <- mydata$workclass
mydata$work<-replace(mydata$work,which(is.na(mydata$work)), rep("other", sum(is.na(mydata$work))))
mydata$age<-replace(mydata$age,which(is.na(mydata$age)), rep(-1000, sum(is.na(mydata$age))))

# make class a factor
mydata$class <- factor(mydata$class)
mydata$work <- factor(mydata$work)
mydata$education <- factor(mydata$education)


# pre processing
mydataWrapped<-WrapData(mydata)
mydataWrapped<-ZScoreXform(mydataWrapped,xformInfo="age->age_z")


# train model
myTree <- rpart(class ~ age_z + work + education, 
                data = mydataWrapped$data)

# convert to pmmlgetwd
pmmlModel <- pmml(myTree, transform=mydataWrapped)



attributes <- data.frame(c("other"), c("too-lazy-to-study"), c(-1000))
rownames(attributes) <- c("missingValueReplacement")
colnames(attributes) <- c("work", "education", "age")
pmmlModel <- addMSAttributes(pmmlModel, attributes=attributes)
write(toString.XMLNode(pmmlModel), file = "/home/britta/es-token-plugin/src/test/resources/org/elasticsearch/script/tree-small-r.xml")