airflow connections add --conn-description "Connection to Covid data" \
  --conn-type "HTTP" --conn-host "https://raw.githubusercontent.com/" \
  "covid_api"
airflow connections add --conn-description "Connection to Spark" \
  --conn-type "Spark" --conn-host "spark://spark-master" --conn-port 7077 \
  "spark_conn"
airflow connections add --conn-description "Connection to Hive" \
  --conn-type "Hive Server 2 Thrift" --conn-host "hive-server" --conn-port 10000 \
  --conn-login "hive" --conn-password "hive" \
  "hive_conn"
