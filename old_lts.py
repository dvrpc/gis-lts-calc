"""
LEGACY PYTHON SCRIPT
This script pulls the road network from Visum, assigns a level of traffic stress (LTS),
and insrets the resulting table into a PostGIS enabled database. It is designed to be used
after network updates are made in the model, before the network is used for connectivity 
or other analysis. The resulting table replaces the existing table.

The variables listed at the top should be updated with each run.  Attribute names should 
be verrified with new versions.
"""

#UPDATE HERE
versionFilePath = "D:/dvrpc_shared/BikeLTS/Phase3/NetworkUpdates/TIM23_2020_forAnalysis_072920.ver" 


from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE
from env_vars import DATA_ROOT
import VisumPy.helpers as h
import pandas as pd
import geopandas as gpd

#open Visum and load version file
Visum = h.CreateVisum(23)
Visum.LoadVersion(versionFilePath)

#lookup table for lane, speed
#second residential line modified to include speed up to 65 to capture everything
road_index = [
    [(0, 0),   (0,  999)],
    [(-2, -2), (0,  25 )], 
    [(-2, -2), (26, 36 )], 
    [(1, 3),   (0,  25 )],
    [(4, 5),   (0,  25 )],
    [(1, 3),   (26, 34 )],
    [(6, 999), (0,  25 )],
    [(4, 5),   (26, 34 )],
    [(6, 999), (26, 34 )],
    [(1, 3),   (35, 999)],
    [(4, 5),   (35, 999)],
    [(6, 999), (35, 999)],
]

residential_index = [
    [(0, 0),   (0,  999)],
    [(1, 2),   (0,  25 )], 
    [(1, 2),   (26, 65 )], 
]


#create lookup for bike facility type
#numbers re-ordered to act as crosswalk between model bike fac codes and those in the reduction factor table
bikeFac_index = [0, 5, 1, 2, 3, 4, 6, 9]

#from LTS table
#row 0 is filler for roads with no lanes
#column 7 is filler for roads with bike fac  =  (opposite direction of a one way street)
StressLevels = [
    [-1, -1, -1, -1, -1, -1, -1, -1],
    [ 1,  1,  1,  1,  1,  1,  1, -2],
    [ 2,  2,  2,  1,  1,  1,  1, -2],
    [ 2,  2,  2,  1,  1,  1,  1, -2],
    [ 3,  3,  3,  2,  2,  1,  1, -2],
    [ 3,  3,  3,  2,  2,  1,  1, -2],
    [ 4,  4,  4,  3,  2,  2,  1, -2],
    [ 4,  4,  4,  3,  2,  2,  1, -2],
    [ 4,  4,  4,  3,  2,  2,  1, -2],
    [ 4,  4,  4,  3,  3,  2,  1, -2],
    [ 4,  4,  4,  3,  3,  2,  1, -2],
    [ 4,  4,  4,  4,  3,  3,  1, -2],
]

#function to identify the row of the table that the record falls in based on the total number of lanes and the speed
def findRowIndex(lanes, spd, link_type):
    if link_type in (72, 79) and lanes in (1, 2):
        for i, ((minlanes,maxlanes), (lowerspd, upperspd)) in enumerate(residential_index):
            if (minlanes <= lanes and lanes <= maxlanes) and (lowerspd <= spd and spd <= upperspd):
                return i
    else:
        for i, ((minlanes,maxlanes), (lowerspd, upperspd)) in enumerate(road_index):
            if (minlanes <= lanes and lanes <= maxlanes) and (lowerspd <= spd and spd <= upperspd):
                return i

#function to idenfity the column of the table based on the bicycle facility
def bikeFacLookup(fac_code):
    for i, (facility) in enumerate(bikeFac_index):
        if facility == fac_code:
            return i

def make_attribute_list(att_name):
    #get attributes from Visum
    map = h.GetMulti(Visum.Net.Links, att_name)
    #convert map object type to list
    att_name = list(map)
    return att_name
    
No       = make_attribute_list("No")
FromNode = make_attribute_list("FromNodeNo")
ToNode   = make_attribute_list("ToNodeNo")
Length   = make_attribute_list("Length")
TotLanes = make_attribute_list("TotNumLanes") #this may need to be calculated first or on the fly with a new version
BikeFac  = make_attribute_list("Bike_Facility") #this may have a dif name
Speed    = make_attribute_list("SPEED_LTS") #this will need a dif name
LinkType = make_attribute_list("TypeNo")
WKTPoly  = make_attribute_list("WKTPoly")
Slope    = make_attribute_list("SLOPE_PERC") #check name of this
OneWay   = make_attribute_list("IsOneWayRoad")
R_OneWay = make_attribute_list("ReverseLink\IsOneWayRoad")

LinkStress = [0]* len(FromNode)

for i in range(0, len(FromNode)):
	x = bikeFacLookup(BikeFac[i])
	y = findRowIndex(TotLanes[i], Speed[i], LinkType[i])
	LinkStress[i] = StressLevels[y][x]

#combine attributes into geodataframe
df = pd.DataFrame(
    {'No'      : No,
    'FromNode' : FromNode,
    'ToNode'   : ToNode,
    'Length'   : Length,
    'TotLanes' : TotLanes,
    'BikeFac'  : BikeFac,
    'Speed'    : Speed,
    'LinkType' : LinkType,
    'Slope'    : Slope,
    'OneWay'   : OneWay,
    'R_OneWay' : R_OneWay,
    'LTS'      : LinkStress,
    'Geom'     : WKTPoly
    }
)


gs = gpd.GeoSeries.from_wkt(df['Geom'])
gdf = gpd.GeoDataFrame(df, geometry = gs, crs = 26918)

gdf.to_json(DATA_ROOT, drive="GeoJSON")
