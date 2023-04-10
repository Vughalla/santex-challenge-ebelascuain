import re
import os
import json
import time
import requests
from Snowflake import Snowflake


class FootballData(Snowflake):
    def __init__(self, league_code):
        '''For development purposes, these credentials are hardcoded 
        directly into the code.
        In the production environment, they will be obtained through 
        AWS Secrets Manager.

        Initialize a FootballData object with Snowflake class inheritance and 
        API credentials for Football Data API. Initialize Snowflake and 
        define class attributes including a URL for the API, authentication token,
        information on tables created, and the league code.
        '''

        super().__init__()
        self.header = 'X-Auth-Token'
        self.token = '21d186c3724549b2bd5e0c8118641ea9'
        self.urlCompetition = f'https://api.football-data.org/v4/competitions/{league_code}'
        self.urlTeamsPlayers = f'https://api.football-data.org/v4/competitions/{league_code}/teams'
        self.league_code = league_code


    def main(self):
        '''Main method. Call other to load and read data using Snowflake DB and expose endpoints.'''
        tp_response = requests.get(url = self.urlTeamsPlayers, headers={self.header: self.token})
        l_response = requests.get(url = self.urlCompetition, headers={self.header: self.token})

        self.loadTeamsData(tp_response)
        self.loadPlayersData(tp_response)
        self.loadCompetitionData(l_response)
        

    def loadTeamsData(self, response):
        '''This method gets and format team data from Football Data API, append data
        to a list, and then load the data into Snowflake. Create tables if tables are not 
        yet created in Snowflake.
        
        Args:
            response: Response object containing API response data.
        '''

        data_origin = "teams"
        json_data = json.loads(response.text)
        teams_list = []
        schema = None

        for team in json_data["teams"]:
            json_team = {
                "name": team["name"],
                "tla": team["tla"],
                "areaName": team["area"]["name"],
                "shortName": team["shortName"],
                "address": team["address"],
                "team_id": team["id"],
                "league_code": self.league_code
            }
            teams_list.append(json_team)
            if not schema:
                schema = self.inferSchema(json_team, data_origin)
            if not self.isTableCreated(data_origin):
                self.createTables(schema)
        teams_dict = {"data_type": data_origin, "data": teams_list}
        self.loadStage([teams_dict])
        self.loadTables(data_origin, schema)


    def loadPlayersData(self, response):
        '''This method gets and format player data from Football Data API, append data
        to a list, and then load the data into Snowflake. Create tables if tables are not 
        yet created in Snowflake.
        
        Args:
            response: Response object containing API response data.
        '''

        data_origin = "players"
        json_data = json.loads(response.text)
        player_list = []
        schema = None

        for team in json_data["teams"]:
            if len(team["squad"]) > 0:
                for player in team["squad"]:
                    json_player = {
                        "name": player["name"],
                        "position": player["position"],
                        "dateOfBirth": player["dateOfBirth"],
                        "nationality": player["nationality"],
                        "team_name": team["name"],
                        "league_code": self.league_code,
                        "is_coach": False
                    }
                    player_list.append(json_player)
                    if not schema:
                        schema = self.inferSchema(json_player, data_origin)
                    if not self.isTableCreated(data_origin):
                        self.createTables(schema)
            else:
                if team["coach"]["name"]:
                    json_player = {
                        "name": team["coach"]["name"],
                        "position": None,
                        "dateOfBirth": team["coach"]["dateOfBirth"],
                        "nationality": team["coach"]["nationality"],
                        "team_name": team["coach"]["name"],
                        "league_code": self.league_code,
                        "is_coach": True
                    }
                    player_list.append(json_player)
                    if not schema:
                        schema = self.inferSchema(json_player, data_origin)
                    if not self.isTableCreated(data_origin):
                        self.createTables(schema)

        players_dict = {"data_type": data_origin, "data": player_list}
        self.loadStage([players_dict])
        self.loadTables(data_origin, schema)

        


    def loadCompetitionData(self, response):
        '''This method gets and format league data from Football Data API, append data
        to a list, and then load the data into Snowflake. Create tables if tables are not 
        yet created in Snowflake.

        Args:
            response: Response object containing API response data.
        '''

        data_origin = "leagues"
        json_data = json.loads(response.text)
        competition_list = []

        json_league = {
            "name": json_data["name"],
            "code": json_data["code"],
            "areaName": json_data["area"]["name"]
        }
        competition_list.append(json_league)
        schema = self.inferSchema(json_league, data_origin)

        if not self.isTableCreated(data_origin):
            self.createTables(schema)

        leagues_dict = {"data_type": data_origin, "data": competition_list}
        self.loadStage([leagues_dict])
        self.loadTables(data_origin, schema)


    def isTableCreated(self, name):
        '''Checks if any table exists in the Snowflake DB with the name 
        %DIM_FOOTBALL% and returns True if there are any.
        '''

        table_name = f"DIM_FOOTBALL_{name}"
        self.cursor.execute(f"SHOW TABLES LIKE '%{table_name}%'")
        exists = len(self.cursor.fetchall()) > 0
        return exists


    def inferSchema(self, json_data, table):
        '''Takes a dictionary json_data and a string table as arguments 
        and infers the schema from json_data. 
        It returns a dictionary containing the schema of the table. 
        The keys in the dictionary are the keys in the json_data dictionary, 
        and the values are the corresponding Snowflake data types inferred
        from the Python data types.'''

        schema = {}
        for key in json_data:
            schema[key] = self.get_snowflake_type(type(key))
        schema["table_name"] = table
        return schema


    def createTables(self, schema):
        '''Creates the OBJ and DIM Snowflake tables using the schema defined in the schema argument.'''

        self.cursor.execute("USE ROLE SYSADMIN;")
        tableName = schema["table_name"]
        # Create a Snowflake table based on the inferred schema
        self.cursor.execute(f"CREATE TRANSIENT TABLE IF NOT EXISTS OBJ_FOOTBALL_{tableName} (raw VARIANT);")
        create_table_sql = f"CREATE TABLE IF NOT EXISTS DIM_FOOTBALL_{tableName} ("
        for key in schema.keys():
            if key != "table_name":
                create_table_sql += f"{key} {schema[key]}, "
        create_table_sql = create_table_sql.rstrip(', ')
        create_table_sql += ")"

        self.cursor.execute(create_table_sql)



    def loadStage(self, json_data):
        '''Recives as parameter the data to be loaded into Snowflake and loads it into an Internal Stage.'''

        os.makedirs('/app/data', exist_ok=True)
        data_origin = json_data[0]["data_type"]
        data = json.dumps(json_data[0]["data"])
        epoch = str(int(time.time()))

        with open(f'/app/data/{data_origin}_{epoch}.json', 'w') as f:
            f.write(data)

        self.cursor.execute(f"PUT 'file:///app/data/{data_origin}_{epoch}.json' @INT_STG_FOOTBAL")


    def loadTables(self, table, schema):
        '''Takes the file football_copy.sql as blue print and modify it to create 
        a copy command for the specific table. This copy command copy the data from the Internal Stage into a OBJ table.
        Next, use the "schema" variable received as a parameter and use it to create the necessary query to insert
        records from the OBJ table into the DIM table.
        This is achieved in conjunction with the dim_football_load.sql
        file that works as a blueprint.'''

        self.cursor.execute("USE ROLE SYSADMIN;")
        with open('sql/football_copy.sql', 'r') as sql_file:
            sql_string = sql_file.read()
            obj_query = sql_string.replace(".*PATTERN.*", f'.*{table}.*').replace("TABLE_NAME", table)
            self.cursor.execute(f'TRUNCATE TABLE OBJ_FOOTBALL_{table};')
            self.cursor.execute(obj_query)
        
        dim_query = self.getLoadDimQuery(schema)
        self.cursor.execute(dim_query)

        


    def getLoadDimQuery(self, schema):
        '''This method uses dim_football_load.sql as blueprint to create a new query
        to load data into the specific dim table, it uses schema data to replace columns values and types.'''

        table_name = schema["table_name"]
        del schema["table_name"]
        with open('sql/dim_football_load.sql', 'r') as sql_file:
            sql_query = sql_file.read()

        for key in schema.keys():
            sql_query = re.sub(r'FIELDS_VALUE(?!\w)', f"r.value:{key}::{schema[key]} AS {key},\n\tFIELDS_VALUE", sql_query)
        sql_query = sql_query.replace(",\n\tFIELDS_VALUE", "").replace("TABLE_NAME", table_name)
        return sql_query


    def get_snowflake_type(self, value_type):
        if isinstance(value_type, float):
            return 'FLOAT'
        if isinstance(value_type, int):
            return 'INT'
        elif isinstance(value_type, bool):
            return 'BOOLEAN'
        if isinstance(value_type, (list, dict, tuple)):
            return 'VARIANT'
        else:
            return 'STRING'
