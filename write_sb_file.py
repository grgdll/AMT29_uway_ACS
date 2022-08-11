# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# Script to submit ACS data to SeaBASS
import pdb
import sys
import xarray as xr
import pandas as pd
import numpy as np
import datetime
import ipdb


def rd_amt_ncdf(fn):
    print('reading NetCDF file...')

    amt = xr.open_dataset(fn)

    print('...done')
    return amt

def hdr(amt, fn_cal, fn_docs):
    print('creating header...')

    header = {
    "/begin_header": "",
    "/received =": "",
    "/identifier_product_doi =": "",
    "/investigators =": "Giorgio_DallOlmo,Gavin_Tilstone",
    "/affiliations =": "Plymouth_Marine_Laboratory,OGS",
    "/contact =": "gdallolmo@ogs.it,ghti@pml.ac.uk",
    "/experiment=": "AMT",
    "/cruise =": amt.attrs['cruise_name'],
    "/station =": "NA",
    "/data_file_name =": "",
    "/documents =": fn_docs, # EXPORTS_NA_ACS_inline_ProcessingReport_v20210818.docx, checklist_acs_particulate_inline_EXPORTS_NA.rtf
    "/instrument_model =": "ACS",
    "/instrument_manufacturer =": "SBE",
    "/calibration_files =": fn_cal, #
    "/data_type =": "flow_thru",
    "/data_status =": "preliminary",
    "/start_date =": "yyyymmdd",
    "/end_date =": "yyyymmdd",
    "/start_time =": "HH:MM:SS[GMT]",
    "/end_time =": "HH:MM:SS[GMT]",
    "/north_latitude =": "DD.DDD[DEG]",
    "/south_latitude =": "DD.DDD[DEG]",
    "/east_longitude =": "DD.DDD[DEG]",
    "/west_longitude =": "DD.DDD[DEG]",
    "/water_depth =": "NA",
    "/measurement_depth =": "7",
    "/missing =": "-9999",
    "/delimiter =": "comma",
    "/fields =": "",
    "/units =": "yyyymmdd, hh:mm: ss, degrees, degrees, degreesC, PSU, 1/m, 1/m, 1/m, 1/m, none, ug/L",
    "/end_header": "",
    }

    # save the order in which the fields need to be printed
    order = header.keys()

    # add wavelengths to /fields and 1/m to units
    _fields = "date,time,lat,lon,Wt,sal,"
    _units = "yyyymmdd,hh:mm:ss,degrees,degrees,degreesC,PSU,"

    for iwv in amt.wv.values:# add ap
        _fields = _fields + "ap" + str(iwv) + ","
        _units = _units + "1/m,"
    for iwv in amt.wv.values:# add std_ap
        _fields = _fields + "ap" + str(iwv) + "_sd,"
        _units = _units + "1/m,"
    for iwv in amt.wv.values:# add std_ap
        _fields = _fields + "cp" + str(iwv) + ","
        _units = _units + "1/m,"
    for iwv in amt.wv.values:# add std_cp
        _fields = _fields + "cp" + str(iwv) + "_sd,"
        _units = _units + "1/m,"

    # add final parts to strings
    _fields = _fields + "bincount, Chl_lineheight"
    _units = _units + "none,ug/L"

    # add strings to keys
    header["/fields ="] = _fields
    header["/units ="] = _units

    ### fill in file name
    # extract start and end dates and times
    start_date = pd.to_datetime(str( amt.time.values.min())).strftime('%Y%m%d')
    start_time = pd.to_datetime(str( amt.time.values.min())).strftime('%H:%M:%S[GMT]')
    end_date = pd.to_datetime(str( amt.time.values.max())).strftime('%Y%m%d')
    end_time = pd.to_datetime(str(amt.time.values.max())).strftime('%H:%M:%S[GMT]')

    ##todays_date
    # set a random timestamp in Pandas
    timestamp = pd.Timestamp(datetime.datetime(2021, 10, 10))
    todays_date = pd.to_datetime(timestamp.today()).strftime('%Y%m%d')

    # create variable with name of seabass file
    sb_filename = amt_no.upper() + "_InLine_ACS_" + start_date + "_" + end_date + "_Particulate_v" + todays_date + ".sb"

    # add string to key
    header["/data_file_name ="] = sb_filename

    header["/start_date ="] = start_date
    header["/start_time ="] = start_time

    header["/end_date ="] = end_date
    header["/end_time ="] = end_time

    header["/received ="] = todays_date


    # extract geographic boundaries
    innan = np.where(~np.isnan(amt.uway_lat.values))[0] # non-nan values
    north_latitude = f'{amt.uway_lat.values[innan].max():+07.3f}[DEG]'
    south_latitude = f'{amt.uway_lat.values[innan].min():+07.3f}[DEG]'

    innan = np.where(~np.isnan(amt.uway_long.values))[0] # non-nan values
    east_longitude = f'{amt.uway_long.values[innan].max():+07.3f}[DEG]'
    west_longitude = f'{amt.uway_long.values[innan].min():+07.3f}[DEG]'

    # add strings to keys
    header["/north_latitude ="] = north_latitude
    header["/south_latitude ="] = south_latitude
    header["/east_longitude ="] = east_longitude
    header["/west_longitude ="] = west_longitude

    print('...done')

    return header

def data_table(amt):
    print('creating data table...')

    #### create pandas DataFrame before exporting to csv
    print('     creating dates...')
    dates = [pd.to_datetime(str(idt)).strftime('%Y%m%d') for idt in amt.time.values]
    dates = pd.Series(dates, index = amt.time.values)
    print('     creating times...')
    times = [pd.to_datetime(str(idt)).strftime('%H:%M:%S') for idt in amt.time.values]
    times = pd.Series(times, index = amt.time.values)
    print('     creating pandas dataframes with data...')
    lat = amt['uway_lat'].to_pandas()
    lon = amt['uway_long'].to_pandas()
    sst = amt['uway_sst'].to_pandas()
    sal = amt['uway_sal'].to_pandas()
    acs_ap = amt['acs_ap'].to_pandas()
    acs_ap_u = amt['acs_ap_u'].to_pandas()
    acs_cp = amt['acs_cp'].to_pandas()
    acs_cp_u = amt['acs_cp_u'].to_pandas()
    acs_N = amt['acs_N'].to_pandas()
    acs_chl_debiased = amt['acs_chl_debiased'].to_pandas()

    # remove acs_ap == -9999
    i_acs_ap_good = acs_ap.values[:,10] != -9999

    dates            = dates[i_acs_ap_good]
    times            = times[i_acs_ap_good]
    lat              = lat[i_acs_ap_good]
    lon              = lon[i_acs_ap_good]
    sst              = sst[i_acs_ap_good]
    sal              = sal[i_acs_ap_good]
    acs_ap           = acs_ap[i_acs_ap_good]
    acs_ap_u         = acs_ap_u[i_acs_ap_good]
    acs_cp           = acs_cp[i_acs_ap_good]
    acs_cp_u         = acs_cp_u[i_acs_ap_good]
    acs_N            = acs_N[i_acs_ap_good]
    acs_chl_debiased =   acs_chl_debiased[i_acs_ap_good]

    print('     concatenating Series...')
    amt2csv = pd.concat([dates, times, lat, lon, sst, sal, acs_ap, acs_ap_u, acs_cp, acs_cp_u, acs_N, acs_chl_debiased], axis=1)

    print('     removing NaNs from lat and lon...')
    # remove NaNs from lat
    amt2csv = amt2csv[ amt2csv[2].notnull() ]
    # remove NaNs from lon
    amt2csv = amt2csv[ amt2csv[3].notnull() ]


    print('...done')

    return amt2csv

def export_2_seabass(header, amt2csv):
    print('writing SeaBASS file...')

    fnout = '../sb/' + header['/data_file_name =']

    with open(fnout, 'w') as ict:
        # Write the header lines, including the index variable for
        # the last one if you're letting Pandas produce that for you.
        # (see above).
        for key in header.keys():
            ict.write(key + " " + header[key] + "\n")

        # Just write the data frame to the file object instead of
        # to a filename. Pandas will do the right thing and realize
        # it's already been opened.
        amt2csv.to_csv(ict, header = False,
                            index = False,
                            na_rep = header['/missing ='])

    print('...done')

    return

def run_fcheck():
    print('running fcheck...')


    print('...done')

    return


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if len(sys.argv) == 1:
        print('ERROR: missing path of NetCDF file to process')
    else:
        print(sys.argv[1])
        # extract cruise name
        amt_no = sys.argv[1].split("/")[-1].split("_")[0]

        # calibration file
        fn_cal = sys.argv[2]

        # document files
        fn_docs = sys.argv[3]

        # read ncdf file
        amt = rd_amt_ncdf(sys.argv[1])

        # add cruise no (all caps) to amt xr.dataset
        amt.attrs['cruise_name'] = amt_no.upper()

        # prepare header
        header = hdr(amt, fn_cal, fn_docs)

        # prepare data
        amt2csv = data_table(amt)

        # write file
        export_2_seabass(header, amt2csv)

        # plot map?






