# load libraries
library(SparkR) 
#install spark-1.6.2-bin-hadoop2.6
# spark_install()
library(sparklyr)

library(ggplot2)
library(dplyr)

# Configure
Sys.setenv(SPARK_HOME='/usr/lib/spark')
Sys.setenv(SPARK_HOME_VERSION='1.6.0')
Sys.setenv(HADOOP_CONF_DIR='/etc/hadoop/conf.cloudera.hdfs')
Sys.setenv(YARN_CONF_DIR='/etc/hadoop/conf.cloudera.yarn')

# Connect
sc <- spark_connect(master = "yarn-client")

# From: http://spark.rstudio.com/mllib.html#linear_regression
# Linear regresssion

# nos vamos a llevar algo a spark
iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
iris_tbl

lm_model <- iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  ml_linear_regression(Petal_Length ~ Petal_Width)

iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  collect %>%
  ggplot(aes(Petal_Length, Petal_Width)) +
  geom_point(aes(Petal_Width, Petal_Length), size = 2, alpha = 0.5) +
  geom_abline(aes(slope = coef(lm_model)[["Petal_Width"]],
                  intercept = coef(lm_model)[["(Intercept)"]]),
              color = "red") +
  labs(
    x = "Petal Width",
    y = "Petal Length",
    title = "Linear Regression: Petal Length ~ Petal Width",
    subtitle = "Use Spark.ML linear regression to predict petal length as a function of petal width."
  )

# logistic regression
# Prepare beaver dataset
beaver <- beaver2
beaver$activ <- factor(beaver$activ, labels = c("Non-Active", "Active"))
copy_to(sc, beaver, "beaver")
beaver_tbl <- tbl(sc, "beaver")

glm_model <- beaver_tbl %>%
  mutate(binary_response = as.numeric(activ == "Active")) %>%
  ml_logistic_regression(binary_response ~ temp)

print(glm_model)

#PCA
pca_model <- tbl(sc, "iris") %>%
  select(-Species) %>%
  ml_pca()
print(pca_model)

# Random Forest
rf_model <- iris_tbl %>%
  ml_random_forest(Species ~ Petal_Length + Petal_Width, type = "classification")

rf_predict <- sdf_predict(rf_model, iris_tbl) %>%
  ft_string_indexer("Species", "Species_idx") %>%
  collect ### problemas de configuracion... Error: class(objId) == "jobj" is not TRUE

table(rf_predict$Species_idx, rf_predict$prediction)

# Stop the SparkSession now
spark_disconnect(sc)
