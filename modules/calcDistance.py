from math import radians, cos, sin, asin, sqrt


def distance(lat1, lon1, lat2, lon2, unit='km'):
    # The math module contains a function named
    # unit is presented the result in kilometers (km) or miles (ml)
    # in case of wrong value foe unit, the result will be in kilometers (km)
    
    
    # radians which converts from degrees to radians.
    lon1 = radians(lon1)
    lat1 = radians(lat1)
    lon2 = radians(lon2)
    lat2 = radians(lat2)

    # Haversine formula
    diff_lon = lon2 - lon1
    diff_lat = lat2 - lat1
    a = sin(diff_lat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(diff_lon / 2) ** 2

    c = 2 * asin(sqrt(a))

    # Radius of earth in kilometers. Use 3956 for miles
    if unit == 'km':
        earth_radius = 6371
    elif unit == 'mi':
        earth_radius = 3959
    else:
        earth_radius = 6371

    # calculate the result
    return (c * earth_radius)