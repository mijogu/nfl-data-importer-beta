import os
import json 
import requests 
from dotenv import load_dotenv, find_dotenv

from Database import *

loaded = load_dotenv(find_dotenv())

class TankStatsAPI():
    use_json_server = True
    season = '2023'

    def __init__(self):
        self.host = os.getenv('RAPID_API_HOST')
        self.apikey = os.getenv('RAPID_API_KEY')

        self.headers = {
            "X-RapidAPI-Key": self.apikey,
            "X-RapidAPI-Host": self.host
        }

        if self.use_json_server:
            self.url = "http://localhost:3000"
        else:
            self.url = "https://" + self.host



    def getGameBoxScores(self, game_id, fantasy_points = "false"):
        # "twoPointConversions":"2",
        # "passYards":".04",
        # "passAttempts":"0",
        # "passTD":"4",
        # "passCompletions":"0",
        # "passInterceptions":"-2",
        # "pointsPerReception":".5",
        # "carries":".2",
        # "rushYards":".1",
        # "rushTD":"6",
        # "fumbles":"-2",
        # "receivingYards":".1",
        # "receivingTD":"6",
        # "targets":"0",
        # "defTD":"6"     
        args = {
            "gameID": game_id, # REQUIRED
            "fantasyPoints": fantasy_points
        }
        request = requests.get(f"{self.url}/getNFLBoxScore", headers=self.headers, params=args)
        return request.json()

    def getScheduleGames(self, week = 'all', season = season, season_type = 'reg'):
        # seasonType = pre, post, reg, or all
        args = {
            "week": week,
            "season": season,
            "seasonType": season_type
        }
        request = requests.get(f"{self.url}/getNFLGamesForWeek", headers=self.headers, params=args)        
        return request.json()


class TankStatsImporter():
    tables = {}
    tables['games'] = (
            "CREATE TABLE games( "
            "game_id VARCHAR(20), "
            "away_team VARCHAR(5), "
            "away_score INT, "
            "away_result CHAR(1), "
            "home_team VARCHAR(5), "
            "home_score INT, "
            "home_result CHAR(1), "
            "game_date VARCHAR(10), "
            "game_time VARCHAR(10), "
            "game_week INT, "
            "scores_imported BOOLEAN,  "
            "season INT, "
            "PRIMARY KEY (game_id) "
        )
    tables['boxscores'] = (
            "CREATE TABLE boxscores( "
            "player_id VARCHAR(20), "
            "game_id VARCHAR(20), "
            "team VARCHAR(5), "
            "rushing_td INT, "
            "rushing_yards INT, "
            "carries INT,  "
            "passing_td INT,  "
            "passing_yards INT, "
            "passing_completions INT,  "
            "passing_int INT,  "
            "receiving_td INT,  "
            "receiving_yards INT, "
            "receptions INT,  "
            "targets INT,  "
            "fumbles INT,  "
            "fumbles_lost INT,  "
            "PRIMARY KEY (player_id, game_id)"
        )
    
    def __init__(self):
        pass

    def setupTables(self):
        db = Database()
        for table_name in self.tables:
            db.createTable(table_name, self.tables[table_name])

    # is 'all' working correctly?
    def importScheduleGames(self, week, season, season_type):
        tankstats = TankStatsAPI()
        games = tankstats.getScheduleGames()
        sql = ("INSERT INTO games "
            "(game_id, away_team, home_team, game_date, game_time, game_week, season) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
            
        args = []
        for game in games["body"]:
            try:
                args.append((
                    game["gameID"],
                    game["away"],
                    game["home"],
                    game["gameDate"],
                    game["gameTime"],
                    int(game["gameWeek"]),
                    int(game["season"]),
                ))
            except Exception as exception:
                print(f"{exception} from the following data...")
                print(json.dumps(game))

        db = Database()
        imported = db.insertmany(sql, args)
        print(f"Imported {imported} games")

    # game_id required
    def importBoxScores(self, game_id):
        tankstats = TankStatsAPI()
        boxscores = tankstats.getGameBoxScores(game_id, "false")
        sql = ("INSERT INTO boxscores "
            "(player_id, game_id, team, " 
            "rushing_td, rushing_yards, carries, "
            "passing_td, passing_yards, passing_completions, passing_int, "
            "receiving_td, receiving_yards, receptions, targets, "
            "fumbles, fumbles_lost) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
            
        args = []
        playerStats = boxscores["body"]["playerStats"]
        for playerID in playerStats:
            player = playerStats[playerID]
            hasRelevantStats = False
            try:
                newbox = [
                    player["playerID"],
                    player["gameID"],
                    player["team"],
                ]

                if "Rushing" in player:
                    # print("has Rushing stats")
                    hasRelevantStats = True
                    newbox.extend([
                        int(player["Rushing"]["rushTD"]),
                        int(player["Rushing"]["rushYds"]),
                        int(player["Rushing"]["carries"])
                    ])
                else: 
                    newbox.extend([0, 0, 0])

                if "Passing" in player:
                    # print("has Passing stats")
                    hasRelevantStats = True
                    newbox.extend([
                        int(player["Passing"]["passTD"]),
                        int(player["Passing"]["passYds"]),
                        int(player["Passing"]["passCompletions"]),
                        int(player["Passing"]["int"])
                    ])
                else: 
                    newbox.extend([0, 0, 0, 0])

                if "Receiving" in player:
                    # print("has Receving stats")
                    hasRelevantStats = True
                    newbox.extend([
                        int(player["Receiving"]["recTD"]),
                        int(player["Receiving"]["recYds"]),
                        int(player["Receiving"]["receptions"]),
                        int(player["Receiving"]["targets"])
                    ])
                else:
                    newbox.extend([0, 0, 0, 0])

                if "Defense" in player:
                    # print("has Defense stats")
                    newbox.extend([         
                        int(player["Defense"]["fumbles"]) if "fumbles" in player["Defense"] else 0,
                        int(player["Defense"]["fumblesLost"]) if "fumblesLost" in player["Defense"] else 0
                    ])
                else: 
                    newbox.extend([0, 0])

                if hasRelevantStats:
                    args.append(newbox)
                # print(json.dump(newbox))
            except KeyError as error:
                print(f"Key error for '{error}'")
            except Exception as exception:
                print(f"{exception} from the following data...")
                print(json.dumps(player))

        db = Database()
        imported = db.insertmany(sql, args)
        if imported:
            print(f"Imported {imported} boxscores")
        else: 
            print("Something went wrong.")
            
        self.updateGameResult(boxscores["body"])

    def updateGameResult(self, game_data):
        sql = ("UPDATE games SET " 
            "away_score = %s, "
            "away_result = %s, "
            "home_score = %s, "
            "home_result = %s, "
            "scores_imported = 1 "
            "WHERE game_id = %s")
            
        args = [
            int(game_data["awayPts"]),
            game_data["awayResult"],
            int(game_data["homePts"]),
            game_data["homeResult"],
            game_data["gameID"]
        ]
        db = Database()
        rowcount = db.update(sql, args)
        print(f"Updated {rowcount} row(s) where game_id = {game_data['gameID']}")

