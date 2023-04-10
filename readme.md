# Santex Leagues

Football challenge for Santex solved by Emmanuel P. Belascuain.
This solution was developed using Python (Pandas and Flask among other libraries), SQL, Snowflake as a database engine, and Docker as a container. The reason for choosing these technologies is their efficiency and easy implementation in any working environment.

A Snowflake account was created for this project, and a user with the necessary permissions was created to allow the code to work, and for you to access the UI and verify everything if desired.

        SNOWFLAKE LOGIN
        URL: https://qhfulqu-aab41342.snowflakecomputing.com/console/login
        USERNAME: SANTEX
        PASSWORD: Santex2023

The process is completely functional, in the first run it creates the corresponding tables before populating them. For this, it uses the data brought from the Football API to infer the corresponding schema.

The data retrieval and loading process is done as follows:
When the '/importLeague' endpoint is executed, it calls the corresponding API and reads the schema with the data. If it is the first run, it will create the tables. The desired fields are filtered from the obtained information, and the data is loaded into an Internal Stage of Snowflake. (since this loading method, using a COPY command later, is the most efficient way to copy data to Snowflake).
Once the JSON files are uploaded to the STAGE, two templates obtained from the files in the /sql folder are used. These templates will be modified to create the corresponding query for each table to be loaded, and the queries will be executed. Moving the information from the STAGE to an OBJ table, still in JSON format in a VARIANT column. Subsequently, the JSON is normalized by moving the data to the DIM table.
Subsequently, each of the remaining three endpoints will read data from the DIM tables in Snowflake to return it to the user.

## Requirements

 - [Docker](https://docs.docker.com/engine/install/)
 - [Python](https://www.python.org/downloads/)

## General 

sdadasadada
    
## Instalation

    1. Clone the repository
    2. From the command prompt, navigate to the project folder where the file named Dockerfile is located and execute the following command:

```bash
  docker-compose up --build
```

## Usage

By executing the previous Docker command, the necessary libraries located in the requirements.txt file will be installed. Subsequently, the main.py script will be executed, which will enable port 8080 and expose the corresponding endpoints. The console will wait for the user to execute a POST request.
The addresses to make a request tipically are http://127.0.0.1:8080 or http://172.19.0.2:8080 (they can change on your local machine, so check your console.)
The exposed endpoints are:

    1. /importLeague
    2. /players
    3. /teams
    4. /playersOfTeam

Each endpoint receives specific parameters that must be sent on the Body when making the POST request.


## importLeague
Receives a JSON parameter containing the code of the league to be loaded into the database.



| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `league_code` | `string` | **Required** |

        
        Example: 
        {"league_code": "PL"}



## players
Receives a JSON parameter containing the league code, and returns the corresponding players for that league. An additional optional parameter can be sent to filter players by team name.



| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `league_code` | `string` | **Required** |
| `team_name` | `string` | Opcional |
        
        Example: 
        {"league_code": "WC"}
        {"league_code": "WC", "team_name": "Spain"}


## teams
Receives a JSON parameter containing the name of a team, and returns the information of that team. An additional optional boolean parameter can be sent to add all the players of that team to the response (or coaches if there are no players when the solution was implemented). By default, this boolean is False.



| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `team_name` | `string` | **Required** |
| `get_players` | `boolean` | Opcional |
        
        Example: 
        {"team_name": "Arsenal FC"}
        {"team_name": "Arsenal FC", "get_players": true}
        

## playersOfTeam
Receives a JSON parameter containing the name of a team, and returns a list of players for that team (or coaches if there are no players when the solution was implemented).



| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `team_name` | `string` | **Required** |

        
        Example: 
        {"team_name": "Arsenal FC"}
