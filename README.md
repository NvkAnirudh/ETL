# ETL with Apache Airflow and Apache Kafka
This project includes building data pipelines using Apache Airflow and Kafka separately, the data is loaded into PostgreSQL and SQL Server databases respectively. 

Steps to run Apache Airflow code:
1) Install Apache Airflow
2) Copy the .py file in the Airflow folder that contains all the required definitions of DAGs (Directed Acyclic Graphs) and tasks to the ~airflow/dags folder (which comes along when you install Airflow). For easy access add the path to a variable like AIRFLOW_DAGS using the command export $AIRFLOW_DAGS=~airflow/dags
3) That's it, since the python file is in the dags folder, airflow will recognize it and display it on the Airflow UI.
4) To start the Airflow UI, you should initialize the database from which airflow takes the DAGs using the command airflow db init and you may want to start the scheduler then using the command airflow scheduler. 
5) Once the scheduler is started, the server for the UI can be started using the command airflow webserver --port 8080. 
6) You can then access the UI at http://localhost/8080
7) You can start, stop, and manage the required DAGs in the UI directly :)

Steps to run Apache Kafka code:
1) Install Apache Kafka using the command wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz and unzip the kafka folder using tar -xzf kafka_2.12-2.8.0.tgz.
2) Start the ZooKeeper (a distributed system that manages the brokers in Kafka architecture) and Kafka using the following commands respectively.
    a) cd kafka_2.12-2.8.0
       bin/zookeeper-server-start.sh config/zookeeper.properties
       
    b) cd kafka_2.12-2.8.0
       bin/kafka-server-start.sh config/server.properties
3) Create a topic to which messaged will be sent since the brokers work using topic partitions. Topic can be created using the command cd kafka_2.12-2.8.0
bin/kafka-topics.sh --create --topic example_topic_name --bootstrap-server localhost:9092
4) Run the producer.py and consumer.py files in Kafka folder in separate tabs.
5) Once run, the streaming data will be loaded in SQL Server database with which analysis can be performed in SQL Server Management Studio.

SQL script file contains the basic analysis performed on streaming data loaded in database. 

Happy Coding :)

