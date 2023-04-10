from flask import Flask, request
from Snowflake import Snowflake
from Football import *

app = Flask(__name__)


@app.route('/importLeague', methods=['POST'])
def importLeague():
    try:
        league_code = request.json['league_code']
        f = FootballData(league_code)
        f.main()
        return {"Success": f"Data loaded successfully."}
    except:
        return {"Error": f"league_code missing or wrong."}
    

@app.route('/players', methods=['POST'])
def getPlayersData():
    '''Takes league code as a parameter and returns the players that belong to all
    teams participating in the given league. If the given league code is not present in the
    DB, it respond with an error message. Has optional input to the endpoint to
    filter players also by team name.'''

    try:
        league_code = request.json['league_code']
        team_name = request.json.get('team_name')
        sf = Snowflake()
        if not team_name:
            data = sf.readPlayersData(league_code)
        else:
            data = sf.readPlayersData(league_code, team_name)
        if len(data) > 2:
            return data
        else:
            if not team_name:
                return {"Error": f"League code {league_code} is not on DB."}
            else:
                return {"Error": f"League code {league_code} or Team Name {team_name} are not on DB."}
    except:
        return {"Error": f"league_code is mandatory."}
    

@app.route('/teams', methods=['POST'])
def getTeamsData():
    '''Takes a name and returns the corresponding team. 
    Additionally, if requested in the query, it resolve the players for that team 
    (or coaches, if players are not available at the moment of implementation).'''

    try:
        sf = Snowflake()
        team_name = request.json['team_name']
        get_players = request.json.get('get_players')
        if not get_players:
            data = sf.readTeamsData(team_name)
            return data
        else:
            if isinstance(get_players, bool):
                data = sf.readTeamsData(team_name, get_players)
                return data
            else:
                return {"Error": f"get_players only admits bool values. Has to be True or False."}
    except:
        return {"Error": f"team_name is mandatory."}


@app.route('/playersOfTeam', methods=['POST'])
def getPlayersOfTeam():
    '''Resolve the players for the given team (or coaches,
    if players are not available at the moment of implementation).'''

    try:
        league_name = request.json['team_name']
        sf = Snowflake()
        data = sf.readPlayersOfTeam(league_name)
        return data
    except:
        return {"Error": f"team_name is mandatory."}

app.run(host='0.0.0.0', port=8080)