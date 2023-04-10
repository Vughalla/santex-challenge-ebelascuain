import snowflake.connector
import pandas as pd
import json

class Snowflake:
    def __init__(self):
        '''For development purposes, these credentials are hardcoded 
        directly into the code.
        In the production environment, they will be obtained through 
        AWS Secrets Manager.'''

        account = 'qhfulqu-aab41342'
        user = 'Santex'
        password = 'Santex2023'
        database = 'Santex'
        schema = 'football'
        warehouse = 'COMPUTE_WH'

        self.conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse
        )
        self.cursor = self.conn.cursor()


    def readPlayersData(self, league_code, team_name=None):
        if not team_name:
            self.cursor.execute(f"SELECT * FROM DIM_FOOTBALL_PLAYERS WHERE LEAGUE_CODE = '{league_code}'")
        else:
            self.cursor.execute(f"SELECT * FROM DIM_FOOTBALL_PLAYERS WHERE LEAGUE_CODE = '{league_code}' AND TEAM_NAME = '{team_name}'")
        df = pd.DataFrame.from_records(iter(self.cursor), columns=[x[0] for x in self.cursor.description])
        json_data = json.loads(df.to_json(orient='records'))
        return json_data


    def readTeamsData(self, team_name, get_players=False):
        self.cursor.execute(f"SELECT * FROM DIM_FOOTBALL_TEAMS WHERE NAME = '{team_name}'")
        df = pd.DataFrame.from_records(iter(self.cursor), columns=[x[0] for x in self.cursor.description])
        json_data = json.loads(df.to_json(orient='records'))

        if not get_players:
            return json_data
        else:
            self.cursor.execute(f"SELECT * FROM DIM_FOOTBALL_PLAYERS WHERE TEAM_NAME = '{team_name}'")
            df2 = pd.DataFrame.from_records(iter(self.cursor), columns=[x[0] for x in self.cursor.description])
            list_players_data = json.loads(df2.to_json(orient='records'))
            json_data[0]["Players"] = list_players_data
            return json_data


    def readPlayersOfTeam(self, team_name):
        self.cursor.execute(f"SELECT * FROM DIM_FOOTBALL_PLAYERS WHERE TEAM_NAME = '{team_name}'")
        df = pd.DataFrame.from_records(iter(self.cursor), columns=[x[0] for x in self.cursor.description])
        json_data = json.loads(df.to_json(orient='records'))
        return json_data
    
