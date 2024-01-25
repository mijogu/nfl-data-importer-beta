from Database import *
from Sleeper import *
from TankStats import *

class SideBets:
    pass

# sleeper = SleeperImporter()
# sleeper.setupTables()

tank = TankStatsImporter()
tank.setupTables()
# tank.importScheduleGames('all', '2023', 'reg')
tank.importBoxScores( "20221212_NE@ARI")