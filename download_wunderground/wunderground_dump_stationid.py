#!/usr/bin/env python2

'''
Description:
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

from lxml.html import parse
import csv
import urllib2
from lxml import html
import numbers
import json
from numpy import concatenate
import sys
from multiprocessing import Pool, Manager, cpu_count
import time
from numpy import vstack
import shutil
import argparse

def get_stationids(processes=cpu_count()):
    '''
    default multiprocesses to the number of cpu cores
    '''
    url='http://dutch.wunderground.com/weatherstation/ListStations.asp?selectedCountry=Netherlands'
    page = parse(url)
    # header 
    rows = page.xpath(".//table[@id='pwsTable']/thead/tr")
    header = [c.text for idx, c in enumerate(rows[0].getchildren())][:-1]
    # add location/zipcode to header
    header = concatenate((header, ['lat', 'lon', 'height', 'zipcode']))
    rows = page.xpath(".//table[@id='pwsTable']/tbody/tr")
    data = list()
    for row in rows:
        data.append([c.text if idx>0 else c.getchildren()[0].text for idx,
                     c in enumerate(row.getchildren())])
    data = [row[:-1] for row in data]
    pool = Pool(processes) # process per core
    m = Manager()
    q = m.Queue()
    args = [(i, q) for i in data]
    result = pool.map_async(append_location_zipcode, args)
    # monitor loop
    while True:
        if result.ready():
            progressbar2(len(data), len(data), prefix="Extracting: ", size=60)
            sys.stdout.write("\n")
            sys.stdout.flush()
            break
        else:
            length = q.qsize()            
            progressbar2(length, len(data), prefix="Extracting: ", size=60)
            time.sleep(1)
    data_out = result.get()
    # clean up
    pool.close()
    pool.join()
    # check if the output datat and header have the same dimension
    if len(data_out[0]) == len(header):
        # add the header to the output data if they have the same dimension
        data_out = vstack((header, data_out))
    return data_out
        
def append_location_zipcode(args):
        # get the location of the station using the stationid
        # example:
        # location = {'lat': 52.235, 'lon': 4.814, 'height': 3.0}
        row, q = args
        location = get_station_location(row[0])
        # get the zipcode using the lon/lat location and googlemaps
        zipcode = get_station_zipcode(location)
        row = concatenate((row,[location['lat'], location['lon'], location['height'], zipcode]))
        q.put(row)
        return [c.encode('utf-8').strip() for c in row]
        
def dump_stationids(data_out, csvfile):
    '''
    write station data to output csv file
    '''
    # move file to ${csvfile}.backup if the csv file already exists
    if os.path.isfile(csvfile):
        shutil.move(csvfile, csvfile + '.backup')
    # write data to csv file
    with open(csvfile, 'w') as fp:
        a = csv.writer(fp, delimiter=',')
        a.writerows(data_out)

def get_station_location(stationid):
    '''
    get the location of a Wunderground stationid
    '''
    # set url to get the location from
    url = 'http://dutch.wunderground.com/personal-weather-station/dashboard?ID=' + stationid
    # open and read url
    handler = urllib2.urlopen(url)
    content = handler.read()
    # find the correct html tag that has the location info in it
    tree = html.fromstring(content).find_class('subheading')
    # get the string of the location
    if len(tree) == 1:
        raw_location = str(tree[0].text_content())
    else:
        raise IOError('Cannot parse location from html file')
    # remove anything non-numeric from the string and create a list
    location_list = [float(s) for s in raw_location.split() if
                     is_number(s)]
    location_items = ['lat', 'lon', 'height']
    # create a dictionary for the location
    location = dict(zip(location_items, location_list))
    # check if latitude and longitude are not zero
    if int(location['lat']) == 0 or int(location['lon']) == 0:
        raise ValueError('Could not extract a valid location for ' +
                         'stationid: ' + stationid)
    return location
    
def get_station_zipcode(location):
    '''
    get zipcode for a given location
    location['lat'] gives latitude of the location
    location['lon'] gives the longitude of the location
    note: there is a limit in api calls/day you can make
    '''
    # google maps api url
    url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + \
        str(location['lat']) + ',' + str(location['lon'])
    # open url
    handler = urllib2.urlopen(url)
    # load json
    js = json.load(handler)
    # extract the address_component
    try:
        address_components = js['results'][0]['address_components']
    except IndexError:
        # can't extract zipcode, invalid location?
        zipcode = 'unknown'
        return zipcode.encode('utf-8')
    # extract the zipcode from the address component
    try:
        zipcode = [address_components[x]['long_name'] for x in
                   range(0, len(address_components)) if
                   address_components[x]['types'][0] == 'postal_code'][0]
    except IndexError:
        # cannot find zipcode
        zipcode = 'unknown'
        # return the zipcode
    return zipcode.encode('utf-8')
        
def is_number(s):
    '''
    check if the value in the string is a number and return True or False
    '''
    try:
        float(s)
        return True
    except ValueError:
        pass
    return False

def progressbar(it, prefix="", size=60):
    '''
    progressbar for a loop
    '''
    count = len(it)

    def _show(_i):
        x = int(size*_i/count)
        sys.stdout.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x),
                                               _i, count))
        sys.stdout.flush()
    _show(0)
    for i, item in enumerate(it):
        yield item
        _show(i+1)
    sys.stdout.write("\n")
    sys.stdout.flush()

def progressbar2(_i, count, prefix="", size=60):
    '''
    progressbar for a loop
    '''
    x = int(size*_i/count)
    sys.stdout.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x),
                                            _i, count))
    sys.stdout.flush()

if __name__ == "__main__":
    # define argument menu
    description = 'Extract all Wunderground stations in the Netherlands ' + \
        'and write the station names and locations to a csv file'
    parser = argparse.ArgumentParser(description=description)
    # fill argument groups
    parser.add_argument('-o', '--output', help='CSV output file',
                        default='wunderground_stations.csv', required=False)
    # extract user entered arguments
    opts = parser.parse_args()
    
    # process data
    process_raw_data(opts)

    stationdata = get_stationids(processes=8)
    dump_stationids(stationdata, opts.output)
