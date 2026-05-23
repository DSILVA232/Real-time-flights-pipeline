At first i was unsure how to build this but i have decided to build 1 layer at a time 

Speed Layer:   Kafka → Spark Streaming → S3 → Snowflake RAW
Batch Layer:   Airflow DAG → S3 → Snowflake RAW
                                        ↓
                              dbt (staging → OBT mart)
                                        ↓
                                 Serving Layer


I have also decided that i will use open sky as my speed layer for real flight data , and ill use something like aviationStack or OpenFlights  for my batch/history layer


KAFKA set up under docker compose yaml , decided to set kafka's replication factor to 1 meaning no metadata backup so things like which consumers are have which events and the like will be lost in the event kafka godes downm, this is fine for this project as its a personal project for production grade  level backups would be recomended (default being 3) and the number of nodes would also need to be increased to more than 1 since running 1 node with 3 backups would leed to issues later