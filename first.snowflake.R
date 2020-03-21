install_dir = "./r_packages/"
.libPaths(c(.libPaths(),install_dir))
#install.packages(c("devtools", "dbplyr", "dplyr", "Rcpp", "withr"), repos='http://mran.revolutionanalytics.com/snapshot/2019-01-06/', lib=install_dir)
library(withr)
#library()
#library(lib.loc = c(.libPaths(),install_dir))
#install.packages("dplyr", repos='http://cran.us.r-project.org', lib=install_dir)
print(.libPaths())
#library(devtools, lib.loc = c(.libPaths(),install_dir), verbose = TRUE)

#with_libpaths(new=install_dir, install_github("snowflakedb/dplyr-snowflakedb"))
#install_version(package = "curl", version = "3.0", repos = "https://cran.r-project.org/", lib=install_dir)
#with_libpaths(new=install_dir, install_github('adjust/api-client-r', upgrade = "never"))

#install.packages(c("rJava","RJDBC", "lubridate"), repos='http://cran.us.r-project.org', lib=install_dir)

require(lubridate)
library(RJDBC)
library(dplyr)
library(dplyr.snowflakedb)
snowflake.jar.loc = paste0(install_dir,"snowflake-jdbc-3.9.2.jar")
download.file("https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.2/snowflake-jdbc-3.9.2.jar", snowflake.jar.loc)
options(dplyr.jdbc.classpath = snowflake.jar.loc)
my_db <- src_snowflakedb(user=Sys.getenv('SNOWFLAKE_USER'), password=Sys.getenv('SNOWFLAKE_PWD'), account="ng95977", region="us-west-2", opts=list(database="BUSINESS_DB", warehouse="ENGINEERING_WH", schema="BI_TRAINING"))
grab_adj_metrics <- function(start_input="2019-07-01", end_input="2019-07-01") {
  require(adjust)
  adjust.setup(user.token = "Hr38eTXxHd6Enz2_jBaK", 
               app.tokens = c('xlygudfrqxhp','o82x3sz1m8zk','hmeuim6v6pz4')
  )
  adj.metrics <- adjust.deliverables(start_date=start_input, end_date=end_input, 
                                     grouping=c('app', 'networks', 'campaigns', 'adgroups', 'creatives', 'hour'),
                                     kpis=c('impressions', 'clicks', 'sessions', 'installs', 'cost')
  )
  return(adj.metrics)
}

#start_date <- as.Date(now())-1
#end_date <- as.Date(now())-1
jdbcDriver <- JDBC(driverClass="com.snowflake.client.jdbc.SnowflakeDriver", classPath="/Users/gracesitienei/Documents/snowflake-jdbc-3.9.2.jar")
jdbcConnection <- dbConnect(jdbcDriver, "jdbc:snowflake://ng95977.snowflakecomputing.com", "grace.sitienei@tala.co",authenticator="externalbrowser",dbname="BUSINESS_DB")

all.dates<-seq(today()-8-21,today(),'day')

for (i in seq(length(all.dates))) {
  start_date <- all.dates[i]
  end_date <- all.dates[i]
  print(now())
  print(paste0("Now pulling data from adj for: ", all.dates[i]))
  all.adj.mets <- grab_adj_metrics(start_date,end_date)
  print(paste0("Now deleteing data for: ", all.dates[i]))
  delete.qry <- paste0("delete
from business_db.bi_training.raw_adj_cost
where spend_date_hour >= date '",start_date,"' and spend_date_hour < date '", start_date + 1, "'")
  result <- dbGetQuery(jdbcConnection, delete.qry)
  print(result)
  print(paste0("Now pushing cost to SnowFlake"))
  copy_to(my_db, all.adj.mets, "RAW_ADJ_COST",mode="append")
  print(paste0(nrow(all.adj.mets)," rows inserted."))
}
#p
#result <- dbGetQuery(jdbcConnection, 'call BUSINESS_DB.BI_REPORTING.load_agg_marketing_performance_mx()')
#print(result)
#result <- dbGetQuery(jdbcConnection, 'call BUSINESS_DB.BI_REPORTING.load_agg_marketing_performance_ph()')
#print(result)
#result <- dbGetQuery(jdbcConnection, 'call BUSINESS_DB.BI_REPORTING.load_agg_marketing_performance_ke()')
#print(result)
#print(result)
#dbCreateTable(jdbcConnection,'TEST_ADJ_COST',all.adj.mets)
