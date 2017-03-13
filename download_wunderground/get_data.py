#!/usr/bin/env python2

'''
Description:
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

from datetime import date
from datetime import timedelta
import urllib2
import htmllib
import cStringIO
import formatter
import os
import sys
from lxml import html
import numbers
import json
import csv
from multiprocessing import Pool, Manager, cpu_count
import time
from datetime import datetime
import download_wunderground.utils as utils
import logging

class get_wundergrond_data:
    def __init__(self, opts):
        self.outputdir = opts.outputdir
        self.stationid = opts.stationid
        self.csvfile = opts.csvfile
        self.keep = opts.keep
        self.startdate = self.validate_date(opts.startdate)
        self.enddate = self.validate_date(opts.enddate)
        self.processes = 8  # number of simultaneous processes/downloads
        logger = logging.getLogger()
        global logger
        if not any([opts.stationid, self.csvfile]):
            raise IOError('stationid or csv file with stationids should ' +
                          'be specified')
        if self.csvfile:
            self.load_csvfile()
            if not opts.stationid:
                stationids = self.csvdata['Station ID']
        else:
            stationids = [opts.stationid]
        for self.stationid in stationids:
            self.outputdir = os.path.join(opts.outputdir, self.stationid)
            if not os.path.exists(self.outputdir):
                os.makedirs(self.outputdir)
            self.get_data_multiprocessing()

    def validate_date(self, datestring):
      '''
      return datetime object from datestring YYYYMMDD
      '''
      return datetime.strptime(datestring, "%Y%m%d")

    def get_data(self):
        '''
        Download data from Weather Underground website for a given stationid
            , a startyar, and an endyear. The html file is parsed and written
            as csv to a separate txt file for each day.
            [singleprocessing code, deprecated]
        '''
        logger.info('Download data for stationid: ' + self.stationid +
                    ' [start]')
        for td in utils.progressbar(range(0, (self.enddate - self.startdate)
                                          .days + 1), "Downloading: ", 60):
            # increase the date by 1 day for the next download
            current_date = self.startdate + timedelta(days=td)
            # set download url
            url = 'http://www.wunderground.com/weatherstation/WXDailyHistory.asp?ID=' + \
                self.stationid + '&day=' + str(current_date.day) + '&year=' + \
                str(current_date.year) + '&month=' + \
                str(current_date.month) + '&format=1'
            # define outputfile
            outputfile = self.stationid + '_' + str(current_date.year) \
                + str(current_date.month).zfill(2) + \
                str(current_date.day).zfill(2) + '.txt'
            # check if we want to keep previous downloaded files
            if self.keep:
                if os.path.exists(os.path.join(self.outputdir, outputfile)):
                    # check if filesize is not null
                    if os.path.getsize(os.path.join(self.outputdir,
                                                    outputfile)) > 0:
                        # file exists and is not null, continue next iteration
                        continue
                    else:
                        # file exists but is null, so remove and redownload
                        os.remove(os.path.join(self.outputdir, outputfile))
            elif os.path.exists(os.path.join(self.outputdir, outputfile)):
                os.remove(os.path.join(self.outputdir, outputfile))
            # open outputfile
            with open(os.path.join(self.outputdir, outputfile),
                      'wb') as outfile:
                # open and read the url
                handler = urllib2.urlopen(url)
                content = handler.read()
                # convert spaces to non-breaking spaces
                content = content.replace(' ', '&nbsp;')
                # Removing all the HTML tags from the file
                outstream = cStringIO.StringIO()
                parser = htmllib.HTMLParser(
                    formatter.AbstractFormatter(
                        formatter.DumbWriter(outstream)))
                parser.feed(content)
                # convert spaces back to regular whitespace (' ')
                content = outstream.getvalue().replace('\xa0', ' ')
                # write output
                outfile.write(content)
                # close handler and outstream
                outstream.close()
                handler.close()
            logger.info('Download data for stationid: ' + self.stationid +
                        ' [completed]')

    def get_station_location(self, stationid):
        '''
        get the location of a Wunderground stationid
        '''
        logger.info('Get station location of stationid: ' + stationid)
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
            logger.error('Cannot parse location from html file')
            raise IOError('Cannot parse location from html file')
        # remove anything non-numeric from the string and create a list
        location_list = [float(s) for s in raw_location.split() if
                         utils.is_number(s)]
        location_items = ['lat', 'lon', 'height']
        # create a dictionary for the location
        location = dict(zip(location_items, location_list))
        # check if latitude and longitude are not zero
        if int(location['lat']) == 0 or int(location['lon']) == 0:
            logger.error('Could not extract a valid location for ' +
                         'stationid: ' + stationid)
            raise ValueError('Could not extract a valid location for ' +
                             'stationid: ' + stationid)
        return location

    def get_station_zipcode(self, location):
        '''
        get zipcode for a given location
        location['lat'] gives latitude of the location
        location['lon'] gives the longitude of the location
        '''
        logger.info('Get zipcode of stationid: ' + self.stationid)
        # google maps api url
        url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + \
            str(location['lat']) + ',' + str(location['lon'])
        # open url
        handler = urllib2.urlopen(url)
        # load json
        js = json.load(handler)
        # extract the address_component
        address_components = js['results'][0]['address_components']
        # extract the zipcode from the address component
        zipcode = [address_components[x]['long_name'] for x in
                   range(0, len(address_components)) if
                   address_components[x]['types'][0] == 'postal_code'][0]
        # return the zipcode
        return zipcode.encode('utf-8')

    def load_csvfile(self):
        '''
        load data csvfile
        '''
        logger.info('Load stationdata from csv file')
        with open(self.csvfile, 'r') as csvin:
            reader = csv.DictReader(csvin, delimiter=',')
            try:
                self.csvdata
            except AttributeError:
                reader.next()
                try:
                    self.csvdata = {k.strip(): [utils.fitem(v)] for k, v in
                                    reader.next().items()}
                except StopIteration:
                    pass
            current_row = 0
            for line in reader:
                current_row += 1
                if current_row == 1:  # header
                    # skip the header
                    continue
                for k, v in line.items():
                    if k is not None:  # skip over empty fields
                        k = k.strip()
                        self.csvdata[k].append(utils.fitem(v))

    def get_data_multiprocessing(self):
        '''
        Download data from Weather Underground website for a given stationid
            , a startyar, and an endyear. The html file is parsed and written
            as csv to a separate txt file for each day.
            [multiprocessing code]
        '''
        logger.info('Download data for stationid: ' + self.stationid +
                    ' [start]')
        pool = Pool(self.processes)  # number of processes
        m = Manager()
        q = m.Queue()
        args = [(self.stationid, self.startdate, td, self.outputdir,
                 self.keep, q) for td in range(0, (
                     self.enddate - self.startdate).days + 1)]
        ndays = range(0, (self.enddate - self.startdate).days + 1)
        result = pool.map_async(get_daily_wunderground, args)
        # monitor loop
        while True:
            if result.ready():
                utils.progressbar2(len(ndays), len(ndays),
                                   prefix="Downloading " +
                                   self.stationid + ": ", size=60)
                sys.stdout.write("\n")
                sys.stdout.flush()
                break
            else:
                length = q.qsize()
                utils.progressbar2(length, len(ndays),
                                   prefix="Downloading " +
                                   self.stationid + ": ", size=60)
                time.sleep(1)
        # clean up
        pool.close()
        pool.join()

def get_daily_wunderground(args):
    '''
    Download Wunderground for a supplied station and date.
    Input argument args consists of (stationid, startdate, td, outputdir,
        keep, q), where
        stationid: stationid on Wunderground website
        startdate: date from which current date is calculated from using td
        td: timedelta in days from startdate
        outputdir: output directory where files are saved
        keep: True if already downloaded files of not size NULL are kept
        q: iterator queue for multiprocessing
    '''
    # input arguments of the function
    stationid, startdate, td, outputdir, keep, q = args
    # increase multiprocessing iterator queue for progressbar2
    q.put(td)
    # increase the date by 1 day for the next download
    current_date = startdate + timedelta(days=td)
    # set download url
    url = 'http://www.wunderground.com/weatherstation/WXDailyHistory.asp?ID=' + \
        stationid + '&day=' + str(current_date.day) + '&year=' + \
        str(current_date.year) + '&month=' + \
        str(current_date.month) + '&format=1'
    # define outputfile
    outputfile = stationid + '_' + str(current_date.year) \
        + str(current_date.month).zfill(2) + \
        str(current_date.day).zfill(2) + '.txt'
    # check if we want to keep previous downloaded files
    if keep:
        if os.path.exists(os.path.join(outputdir, outputfile)):
            # check if filesize is not null
            if os.path.getsize(os.path.join(outputdir,
                                            outputfile)) > 0:
                # file exists and is not null, continue next iteration
                return
            else:
                # file exists but is null, so remove and redownload
                os.remove(os.path.join(outputdir, outputfile))
        elif os.path.exists(os.path.join(outputdir, outputfile)):
            os.remove(os.path.join(outputdir, outputfile))
    # open outputfile
    with open(os.path.join(outputdir, outputfile), 'wb') as outfile:
        # open and read the url
        handler = urllib2.urlopen(url)
        content = handler.read()
        # convert spaces to non-breaking spaces
        content = content.replace(' ', '&nbsp;')
        # Removing all the HTML tags from the file
        outstream = cStringIO.StringIO()
        parser = htmllib.HTMLParser(
            formatter.AbstractFormatter(
                formatter.DumbWriter(outstream)))
        parser.feed(content)
        # convert spaces back to regular whitespace (' ')
        content = outstream.getvalue().replace('\xa0', ' ')
        # write output
        outfile.write(content)
        # close handler and outstream
        outstream.close()
        handler.close()
    logger.info('Download data for stationid: ' + stationid +
                ' [completed]')
    return
