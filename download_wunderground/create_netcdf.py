#!/usr/bin/env python2

'''
Description:    Class to combine data of Wunderground csv data into a single
                output file
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:
Last Modified:
License:        Apache 2.0
Notes:          * User gives input directory containing csv txt files of
                  Wunderground data as input
                * User optionally specifies output directory
                * Combined netCDF (or TODO: csv) is created
'''

import csv
import glob
import os
from netCDF4 import Dataset as ncdf
from netCDF4 import date2num as ncdf_date2num
from datetime import datetime
from numpy import zeros
from numpy import argsort
from numpy import array as nparray
from numpy import append as npappend
import time
from dateutil import tz
import argparse
import itertools
from numpy import nan as npnan
from numpy import concatenate as npconcatenate
import download_wunderground.utils as utils

class process_raw_data:
    ''''
    Class to read the raw input data and combine them into a single output
    file
    '''
    def __init__(self, inputdir, outputdir, lat=False, lon=False):
        # set class variables
        self.inputdir = inputdir
        print('Processing ' + self.inputdir)
        self.outputdir = outputdir
        # define filename as basename inputdir with .nc extension
        filename = os.path.basename(self.inputdir) + '.nc'
        self.outputfile = os.path.join(self.outputdir, filename)
        self.lat = lat
        self.lon = lon
        if os.path.exists(os.path.join(self.outputfile)):
            # check if filesize is not null
            if os.path.getsize(os.path.join(self.outputfile)) > 0:
                # file exists and is not null, continue next iteration
                print('Please remove existing netCDF file before recreating: ' 
                  + self.outputfile)
                return
        # do we want to hardcode this?
        self.get_field_names()
        try:
          self.dateUTCstring = [s for s in self.field_names if s is not None
                                and "DateUTC" in s][0]
          #self.field_names.append('<br>')
          # call functions
          self.combine_raw_data()
          self.write_combined_data_netcdf()
        except AttributeError:
          print('Nothing to write for ' + self.outputfile)

    def combine_raw_data(self):
        '''
        combine them
        into single output variable
        '''
        # get a list of all txt files in inputdir, sorted by filename
        filelist = sorted(glob.glob(os.path.join(self.inputdir, '*.txt')))
        if len(filelist) == 0:
            raise IOError('No files found in ' + self.inputdir)
        for inputfile in filelist:
            with open(inputfile, 'r') as csvin:
                reader = csv.DictReader(csvin, delimiter=',')
                try:
                    self.data
                except AttributeError:
                    #reader.next()
                    try:
                        self.data = {k.strip(): [utils.fitem(v)] for k, v in
                                     reader.next().items() if k is not None}
                    except StopIteration:
                        pass
                current_row = 0
                for line in reader:
                    current_row += 1
                    if current_row == 1:  # header
                        # skip the header
                        continue
                    elif line['Time'] == '<br>':
                        # not a valid csv line, so skip
                        continue
                    else:
                        try:
                            datetime.strptime(line[self.dateUTCstring],
                                              ('%Y-%m-%d %H:%M:%S'))
                        except ValueError:
                            # Not a valid csv line, so skip
                            continue
                    lenDateUTC = len(self.data['DateUTC'])
                    for k, v in line.items():
                        if k is not None and k in self.data.keys():  # skip over empty fields
                            k = k.strip()
                            try:
                                addnones = lenDateUTC - len(self.data[k])
                            except NameError:
                                addnones = 0
                            if addnones > 0:
                                toadd = ['' for c in range(0,addnones)]
                                self.data[k] = npconcatenate((
                                    self.data[k], toadd)).tolist()
                            self.data[k].append(utils.fitem(v))
        # check if we need to add empty values at the end of the lists
        lenDateUTC = len(self.data['DateUTC'])
        for var in self.data.keys():
            try:
                addnones = lenDateUTC - len(self.data[var])
            except NameError:
                addnones = 0
            if addnones > 0:
                toadd = ['' for c in range(0,addnones)]
                self.data[var] = npconcatenate((
                    self.data[var], toadd)).tolist()         
        # verify that everything is sorted with time
        if not self.verify_sorting():
            # sort data if needed according to time
            self.sort_data()

    def write_combined_data_netcdf(self):
        ncfile = ncdf(self.outputfile, 'w', format='NETCDF4')
        # description of the file
        ncfile.description = 'Hobby meteorologists data ' + self.inputdir
        ncfile.history = 'Created ' + time.ctime(time.time())
        # create time dimension
        timevar = ncfile.createDimension('time', None)
        # create time variable local time Europe/Amsterdam
        timeaxisLocal = zeros(len(self.data[self.dateUTCstring][1:]))
        # define UTC and local time-zone (hardcoded)
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('Europe/Amsterdam')
        # convert time string to datetime object
        for idx in range(1, len(self.data[self.dateUTCstring])):
            # define time object from string
            timeObject = datetime.strptime(self.data[self.dateUTCstring][idx],
                                           '%Y-%m-%d %H:%M:%S')
            # tell timeObject that it is in UTC
            timeObject = timeObject.replace(tzinfo=from_zone)
            # time axis UTC
            try:
              timeaxis = npappend(timeaxis, ncdf_date2num(
                timeObject.replace(tzinfo=None),
                units='minutes since 2010-01-01 00:00:00',
                calendar='gregorian'))
            except NameError:
              timeaxis = ncdf_date2num(
                timeObject.replace(tzinfo=None),
                units='minutes since 2010-01-01 00:00:00',
                calendar='gregorian')

        # netcdf time variable UTC
        timevar = ncfile.createVariable('time', 'i4', ('time',),
                                        zlib=True)
        timevar[:] = timeaxis
        timevar.units = 'minutes since 2010-01-01 00:00:00'
        timevar.calendar = 'gregorian'
        timevar.standard_name = 'time'
        timevar.long_name = 'time in UTC'

        # write lon/lat variables if available
        if ((self.lat) and (self.lon)):
            lonvar = ncfile.createDimension('longitude', 1)
            lonvar = ncfile.createVariable('longitude', 'float32',('longitude',))
            lonvar.units = 'degrees_east'
            lonvar.axis = 'X'
            lonvar.standard_name = 'longitude'
            lonvar[:] = self.lon
            latvar = ncfile.createDimension('latitude', 1)
            latvar = ncfile.createVariable('latitude', 'float32',('latitude',))
            latvar.units = 'degrees_north'
            latvar.axis = 'Y'
            latvar.standard_name = 'latitude'
            latvar[:] = self.lat
        # create other variables in netcdf file
        for self.variable in self.data.keys():
            if self.variable not in [self.dateUTCstring, 'Time', '<br>', None]:
                # add variables in netcdf file
                # convert strings to npnan if array contains numbers
                if True in [utils.is_number(c)
                            for c in self.data[self.variable]]:
                    self.data[self.variable] = [npnan if isinstance(
                        utils.fitem(c), str) else utils.fitem(c) for c in self.data[
                            self.variable]]
                # check if variable is a string
                if not isinstance(self.data[self.variable][1], str):
                    # fill variable
                    if self.variable == 'SolarRadiationWatts/m^2':
                        #variableName = 'SolarRadiation'
                        continue
                    elif ((self.variable == 'TemperatureC') or
                          (self.variable == 'TemperatureF')):
                        variableName = 'temperature'
                    else:
                        variableName = self.variable
                    self.values = ncfile.createVariable(
                        variableName, type(self.data[self.variable][1]),
                        ('time',), zlib=True, fill_value=-999)
                else:
                    # string variables cannot have fill_value
                    self.values = ncfile.createVariable(
                        self.variable, type(self.data[self.variable][1]),
                        ('time',), zlib=True)
                # TODO: km/h->m/s ??
                try:  # fill variable
                    if not self.variable in ['TemperatureC', 'TemperatureF']:
                      self.values[:] = self.data[self.variable][1:]
                    elif self.variable == 'TemperatureC':
                      self.values[:] = 273.15 + nparray(self.data[self.variable][1:])
                    elif self.variable == 'TemperatureF':
                      self.values[:] = (nparray(self.data[self.variable][1:]) - 32.)/1.8
                except IndexError:
                    # for strings the syntax is slightly different
                    self.values = self.data[self.variable][1:]
                self.fill_attribute_data()
        ncfile.close()

    def fill_attribute_data(self):
        '''
        Function that fills the attribute data of the netcdf file
        '''
        if self.variable == 'TemperatureC':
            self.values.units = 'K'
            self.values.standard_name = 'air_temperature'
            self.values.long_name = 'air temperature'
        elif self.variable == 'TemperatureF':
            self.values.units = 'K'
            self.values.standard_name = 'air_temperature'
            self.values.long_name = 'air temperature'
        elif self.variable == 'DewpointC':
            self.values.units = 'C'
            self.values.standard_name = 'dew_point_temperature'
            self.values.long_name = 'dewpoint temperature'
        elif self.variable == 'PressurehPa':
            self.values.units = 'hPa'
            self.values.long_name = 'surface pressure'
            self.values.standard_name = 'surface_air_pressure'
        elif self.variable == 'PressureIn':
            pass
        elif self.variable == 'WindDirection':
            pass
        elif self.variable == 'WindDirectionDegrees':
            self.values.units = 'degrees'
            pass
        elif self.variable == 'WindSpeedKMH':
            self.values.units = 'km/h'
            self.values.standard_name = 'wind_speed'
            self.values.long_name = 'wind speed'
        elif self.variable == 'WindSpeedGustKMH':
            self.values.units = 'km/h'
            self.values.standard_name = 'wind_speed_of_gust'
            self.values.long_name = 'gust wind speed'
        elif self.variable == 'Humidity':
            self.values = ''
        elif self.variable == 'HourlyPrecipMM':
            self.values.units = 'mm/h'
            self.values.long_name = 'hourly precipitation'
        elif self.variable == 'Conditions':
            pass
        elif self.variable == 'Clouds':
            pass
        elif self.variable == 'dailyrainMM':
            self.values.units = 'mm/day'
            self.values.long_name = 'daily precipitation'
        elif self.variable == 'SoftwareType':
            pass
        elif self.variable == 'SolarRadiationWatts/m^2':
            self.values.units = 'Watts/m2'
            self.values.long_name = 'solar radiation'
        else:
            pass
            #raise Exception('Unkown field name ' + self.variable)

    def write_combined_data_csv(self):
        '''
        Function to write the output to a csv file
        '''
        pass

    def verify_sorting(self):
        '''
        Function to verify that the data is sorted according to the time axis
        defined by self.data['DateUtC'][1:]
        '''
        if not all(earlier <= later for earlier, later in
                   itertools.izip(self.data[self.dateUTCstring][1:],
                                  self.data[self.dateUTCstring][2:])):
            return False
        else:
            return True

    def sort_data(self):
        '''
        Function to sort the data according to the time axis defined by
        self.data['DateUTC'][1:]
        '''
        idx_sort = argsort(self.data[self.dateUTCstring][1:])
        for field_name in self.data.keys():
            if field_name is not self.dateUTCstring:
                self.data[field_name][1:] = nparray(
                    self.data[field_name][1:])[idx_sort].tolist()

    def get_field_names(self):
        # get a list of all txt files in inputdir, sorted by filename
        filelist = sorted(glob.glob(os.path.join(self.inputdir, '*.txt')))
        for inputfile in filelist:
            with open(inputfile, 'r') as csvin:
                reader = csv.DictReader(csvin, delimiter=',')
                # loader header information
                try:
                    self.field_names = {k: [v] for k, v in
                                        reader.next().items()}
                    self.field_names = self.field_names.keys()
                #self.field_names = self.field_names[None][0][:]
                except StopIteration:
                    pass
                try:
                    reader.next()
                    # first txt file with data in it found
                    # use field_names from this file
                    break
                except StopIteration:
                    pass
