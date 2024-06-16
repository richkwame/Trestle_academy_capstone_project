# Trestle_academy_capstone_project
## Data Ingestion project
### Project Title:
"Real-Time Weather Data Ingestion and Processing Pipeline"

### Project Overview:
The goal of this project is to develop a data ingestion pipeline that fetches real-time weather data from a public API, and stores it in a database for further analysis and visualization.

### Scope and Objectives:
Data Source:  public weather API (OpenWeatherMap API).<br>
Tools and Technologies: Python, Airflow, PostgreSQL, Docker, Dbeaver, Pandas.<br>
Objectives:
Fetch real-time weather data at regular intervals.
Store the data in a PostgreSQL database.
Set up a monitoring and error handling system with email notification to ensure data quality and pipeline reliability.

### Project Implementation:
Data Source:<br>
1.sign up at OpenweatherMap website for API key<br>
2.test the API using code in 'Python_script'<br>
Data Ingestion:<br>
3.using the command Docker-compose up, create a container using postgres and airflow image in Docker after saving the 'Docker-compose.yml' file<br>
4.Ensure the container is running in docker, also check all logs to ensure there are no errors<br>
5.Using http://localhost:8080 , access the airflow web user interface and use admin as username and password<br>
6.Ensure that 'weather_data_ingestion_retrieval' python file is located at the DAG portion in the airflow home page<br>
7. Trigger DAG manaually to test the pipeline manaually<br>
8. The pipeline is scheduled to run automatically every 30 mins<br>
9. Open dbeaver and establish a connection with airflow and search for real_weather_data form the tables in the top left conner of the database created<br>
10. Run a simple query like 'SELECT * FROM real_weather_data' toview the injested data from Openweather with the API<br>
11. Export the data to csv or excel for furthur cleaning.<br>

#### Data Storage:<br>
1.Set up a PostgreSQL connection in Airflow:<br>
2.Navigate to Admin > Connections.<br>
3.Click on the + button to add a new connection.<br>
4.Enter the following details:<br>
Conn Id: postgres_default<br>
Conn Type: Postgres<br>
Host: postgres<br>
Schema: airflow<br>
Login: airflow<br>
Password: airflow<br>
Port: 5432<br>
#### Monitoring
1.Monitor the pipeline to ensure its working successfuly. The data generated can be exported for cleaning and furthur analysis<br>
2.Using data base management tools such as Dbeaver connect to airflow and view the data generated<br>
using these connection properties:<br>
Host: localhost (or the IP address of your PostgreSQL server if it is not on the same machine)<br>
Port: 5432<br>
Database: airflow (or the name of your PostgreSQL database)<br>
Username: airflow (or the PostgreSQL username you set)<br>
Password: airflow (or the PostgreSQL password you set)<br>

