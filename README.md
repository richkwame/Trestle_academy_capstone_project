# Trestle_academy_capstone_project
## Data Ingestion project
### Project Title:
"Real-Time Weather Data Ingestion and Processing Pipeline"

### Project Overview:
The goal of this project is to develop a data ingestion pipeline that fetches real-time weather data from a public API, and stores it in a database for further analysis and visualization.

### Scope and Objectives:
Data Source:  public weather API (OpenWeatherMap API).<br>
Tools and Technologies: Python, Airflow, PostgreSQL, Docker, Pandas.<br>
Objectives:
Fetch real-time weather data at regular intervals.
Store the data in a PostgreSQL database.
Set up a monitoring and error handling system with email notification to ensure data quality and pipeline reliability.

### Project Implementation:
#### Data Source:<br>
1.sign up at OpenweatherMap website for API key<br>
2.test the API using code in 'Python_script'<br>
Data Ingestion:<br>
3.using the command Docker-compose up, create a container using postgres and airflow image in Docker after saving the 'Docker-compose.yml' file<br>
4.Ensure the container is running in docker, also check all logs to ensure there are no errors<br>
5.Using http://localhost:8080 , access the airflow web user interface and use admin as username and password
6.Ensure that 'weather_data_ingestion_retrieval' python file is located at the DAG portion in the airflow home page

#### Data Storage:<br>
Set up a PostgreSQL connection in Airflow:<br>
Navigate to Admin > Connections.<br>
Click on the + button to add a new connection.<br>
Enter the following details:<br>
Conn Id: postgres_default<br>
Conn Type: Postgres<br>
Host: postgres<br>
Schema: airflow<br>
Login: airflow<br>
Password: airflow<br>
Port: 5432<br>
#### Monitoring
Monitor the pipeline to ensure its working successfuly. The data generated can be exported for cleaning and furthur analysis
