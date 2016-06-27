library(pmml)
library(pmmlTransformations)


# a hack to get cwd
script.dir <- getSrcDirectory(function(x) {
  x
})
rmlist = (ls())
script <- paste(script.dir, "/dataHelperFunctions.R", sep = "")
result.dir <-
  paste(script.dir, "/../org/elasticsearch/script/", sep = "")
source(script)

# load data, replace missing values and covert string to factors
mydata <- prepareData()

# pre processing
mydataWrapped <- WrapData(mydata)
mydataWrapped <- ZScoreXform(mydataWrapped, xformInfo = "age->age_z")
mydataWrapped <-
  ZScoreXform(mydataWrapped, xformInfo = "education_num->education_num_z")
mydataWrapped <-
  ZScoreXform(mydataWrapped, xformInfo = "hours_per_week->hours_per_week_z")

# train model
mylogit <-
  glm(
    class ~ age_z + workclass + education + education_num_z + marital_status 
    + occupation + relationship + race + sex + hours_per_week_z 
    + native_country
    ,
    data = mydataWrapped$data,
    family = "binomial",
    na.action = na.pass
    
  )

# convert to pmml
pmmlModel <- pmml(mylogit, transform = mydataWrapped)

# edit model to add the missing value replacement
attributes <-
  data.frame(c("too-cool-to-work"), c("hedonist"), c("Fiji"))
rownames(attributes) <- c("missingValueReplacement")
colnames(attributes) <-
  c("workclass", "occupation", "native_country")
pmmlModel <- addMSAttributes(pmmlModel, attributes = attributes)

# finally write the model as xml file
write(toString.XMLNode(pmmlModel),
      file = paste(result.dir, "glm-adult-full-r.xml", sep = ""))

# for tests output the predicted class and probs to a file as well
prob <-
  predict(mylogit, newdata = mydataWrapped$data, type = "response")
result <- sapply(prob, function(x)
  if(x > 0.5) {
    ">50K"
  } else{
    "<=50K"
  })
compresult = data.frame(sapply(prob, function(x) {
  1.0 - x
}), prob,  result)
colnames(compresult) <-
  c("probClass0", "probClass1" , "predictedClass")

write.table(
  compresult,
  file = paste(result.dir, "r_glm_adult_result.csv", sep = ""),
  row.names = F,
  sep = ","
  
)
