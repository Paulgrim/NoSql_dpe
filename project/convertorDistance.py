from pyproj import Transformer
from math import ceil
import json

transformerto3035 = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
transformerto84 = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)


def wgs84_to_epsg3035(long: float, lat: float):
    """
    convert longitude latitude to X, Y
    EPSG:4326 => EPSG:3035
    :param long: longitude float
    :param lat: latitude float
    :return: x, y
    """
    return transformerto3035.transform(long, lat)


def epsg3035_to_wgs84(x: float, y: float):
    """
    convert X, Y to longitude latitude
    EPSG:4326 => EPSG:3035
    :param x: float
    :param y: float
    :return: longitude, latitude
    """
    return transformerto84.transform(x, y)


def convertMtoKM(m: float, size: int):
    """
    convert meters to km, and return km - (km modulo size)
    :param m: float
    :param size: integer
    :return: integer
    """
    km = ceil(m / 1000)
    return km - km % size


def geometry(x: int, y: int, density: float, size: int):
    """
    create square for geoJson
    :param x: integer coordinate x
    :param y:  integer coordinate y
    :param density: float
    :param size: integer
    :return: dictionary for Polygon (geoJson)
    """
    poly = {
        "type": "Polygon",
        "coordinates": []
    }
    id = str(x) + str(y)
    x = 1000 * x
    y = 1000 * y
    square = size * 1000
    x1, y1 = epsg3035_to_wgs84(x + square, y)
    x2, y2 = epsg3035_to_wgs84(x, y)
    x3, y3 = epsg3035_to_wgs84(x, y + square)
    x4, y4 = epsg3035_to_wgs84(x + square, y + square)
    coord = [[x1, y1], [x2, y2], [x3, y3], [x4, y4]]
    poly["coordinates"].append(coord)
    return {
        "type": "Feature",
        "properties": {"name": str(density)},
        "id": id,
        "geometry": poly}


def createGeospace(df, size: int):
    """
    create geoJson file with squares
    :param df: pandas.DataFrame object
    :param size: integer
    :return:
    """
    geospace = {
        "type": "FeatureCollection",
        "features": []
    }
    for row in df.itertuples():
        geospace["features"].append(geometry(row.x, row.y, row.densite, size))
    file = "data/geospace" + str(size) + "km.json"
    with open(file, "w") as f:
        json.dump(geospace, f)
