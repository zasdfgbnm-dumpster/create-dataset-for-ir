#!/usr/bin/python
import os
import urllib.request

# this is the script to download raw data from nist

# On each step, the process information will be written to 'outputs/01'
# directory. If this script is terminated before finish, the next time
# when this script was run, it will automatically find the current progress
# and continue

# functions and variables to help determine continue step
outputdir = 'outputs/01'
mass_ranges_fn = outputdir + '/mass_ranges'
idlist_fn = outputdir + '/idlist'
intralist_fn = outputdir + '/intralist'

def test_outputdir():
    return os.path.isdir(outputdir)

def check_file_done(filename):
    if not os.path.isfile(filename):
        return False
    with open(filename) as f:
        for line in f:
            if line.strip() == 'done':
                return True
    return False


# step 0: create outputs/01
if not test_outputdir():
    os.makedirs(outputdir)


# step 1: find ranges of masses to do search on NIST in order to get
url1 = 'http://webbook.nist.gov/cgi/cbook.cgi?Value='
url2 = '500-99999' #just an example
url3 = '&VType=MW&Units=SI&cIR=on'
def query_list(l,r):
    url2 = '{}-{}'.format(l,r)
    data = urllib.request.urlopen(url1+url2+url3).read().decode("utf-8")
    # test no results
    idx = data.find('Chemical Formula Not Found')
    if idx != -1:
        return 'no results',data
    # test too large
    idx = data.find('Due to the large number of matching species, only the first')
    if idx != -1:
        return 'too large',data
    # test multiple results
    idx = data.find('matching species were found.')
    if idx != -1:
        return 'multiple results',data
    # test single result
    idx = data.find('Information on this page:')
    if idx != -1:
        return 'single result',data
    raise Exception('unrecognizable result type')

def range_size_is_good(starting,ending):
    result_type,data = query_list(starting,ending)
    return result_type != 'too large'

def write_ranges(ranges):
    with open(mass_ranges_fn,'w') as f:
        for x,y in ranges:
            f.write('{} {}\n'.format(x,y))
        f.write('done')

if not check_file_done(mass_ranges_fn):
    ranges = []
    lowerlimit = 0
    upperlimit = 99999
    decay_rate = 0.7
    digits = 3

    print('[start] looking for correct mass ranges...')
    starting,ending = lowerlimit,upperlimit
    while ending > starting:
        while not range_size_is_good(starting,ending):
            ending = round(ending*decay_rate + starting*(1-decay_rate), digits)
        ranges += [(starting,ending)]
        print('new range:',(starting,ending))
        starting,ending = ending,upperlimit
    write_ranges(ranges)
    print('[ end ] looking for correct mass ranges...')


# step 2: do search on NIST, make a list of structures with experimental ir data
def handle_multiple_results(data):
    idlist = []
    items = data.split('<a href="/cgi/cbook.cgi?ID=')[1:]
    for i in items:
        myid = i.split('&amp;')[0]
        idlist.append(myid)
    return idlist

def handle_single_results(data):
    raise Exception('single result handler is not implemented')

def read_mass_ranges():
    ranges = []
    with open(mass_ranges_fn) as f:
        for line in f:
            line = line.strip()
            if line == 'done':
                break
            starting,ending = tuple([float(x) for x in line.split()])
    return ranges

if not check_file_done(idlist_fn):
    ranges = read_mass_ranges()

# step 3: for each structure, make a list of experimental data that NIST have
if not check_file_done(intralist_fn):
    pass

# step 4: download all the raw data, including structure, experimental spectrum
# and theoretical spectrum
