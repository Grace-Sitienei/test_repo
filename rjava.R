dyn.load('/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/lib/server/libjvm.dylib')
library("rJava")
.jinit()
.jcall("java/lang/System", "S", "getProperty", "java.runtime.version")

install.packages("RJDBC",repos='http://cran.us.r-project.org')
library("RJDBC", "DBI")

library(dplyr)
library(dplyr.snowflakedb)

jdbcDriver <- JDBC(driverClass="com.snowflake.client.jdbc.SnowflakeDriver", classPath="/Users/gracesitienei/Documents/snowflake-jdbc-3.9.2.jar")
jdbcConnection <- dbConnect(jdbcDriver, "jdbc:snowflake://ng95977.snowflakecomputing.com", "grace.sitienei@tala.co",authenticator="externalbrowser",dbname="BUSINESS_DB")

all.dates<-seq(today()-8-21,today(),'day')
