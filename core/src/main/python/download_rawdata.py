#!/usr/bin/python
import os,sys,hashlib
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
files_fn = outputdir + '/files'

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
def query_list(l,r):
    url1 = 'http://webbook.nist.gov/cgi/cbook.cgi?Value='
    url2 = '{}-{}'.format(l,r)
    url3 = '&VType=MW&Units=SI&cIR=on'
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
    idlist = set()
    items = data.split('<a href="/cgi/cbook.cgi?ID=')[1:]
    for i in items:
        myid = i.split('&amp;')[0].strip()
        idlist.add(myid)
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
            myrange = tuple([float(x) for x in line.split()])
            ranges.append(myrange)
    return ranges

def write_idlist(idlist):
    with open(idlist_fn,'w') as f:
        for i in idlist:
            f.write(i+'\n')
        f.write('done\n')


if not check_file_done(idlist_fn):
    idlist = set()
    ranges = read_mass_ranges()
    for l,r in ranges:
        mytype,data = query_list(l,r)
        if mytype == 'multiple results':
            newids = handle_multiple_results(data)
        elif mytype == 'single result':
            newids = handle_single_results(data)
        else:
            raise Exception('Unexpected error on getting id list')
        print('{} new items for range {}-{}'.format(len(newids),l,r))
        idlist = idlist | newids
    write_idlist(idlist)

# step 3: for each structure, make a list of files needed to be downloaded
def read_idlist():
    idlist = set()
    with open(idlist_fn) as f:
        for line in f:
            line = line.strip()
            if line == 'done':
                break
            idlist.add(line)
    return idlist

def query_filelist(myid):
    file_list = []
    url1 = 'http://webbook.nist.gov/cgi/cbook.cgi?ID='
    url2 = myid
    url3 = '&Units=SI&Mask=80'
    fullurl = url1+url2+url3
    data = urllib.request.urlopen(fullurl).read().decode("utf-8")
    # test 2d Mol file
    has2d = data.find('2d Mol file') != -1
    if has2d:
        url = 'http://webbook.nist.gov/cgi/cbook.cgi?Str2File={}'.format(myid)
        filename = 'mol_files/{}.mol'.format(myid)
        file_list.append((url,filename))
    # test 3d SD file
    has3d = data.find('3d SD file') != -1
    if has3d:
        url = 'http://webbook.nist.gov/cgi/cbook.cgi?Str3File={}'.format(myid)
        filename = 'sdf_files/{}.sdf'.format(myid)
        file_list.append((url,filename))
    # get a list of experimental spectrum
    irurl_header = 'http://webbook.nist.gov'
    irurl_template = '/cgi/cbook.cgi?ID={}&amp;Units=SI&amp;Type=IR-SPEC&amp;Index={}#IR-SPEC'
    ir_url_list = [(i,irurl_template.format(myid,i)) for i in range(1000)]
    ir_url_list = [ (i,irurl_header+x) for i,x in ir_url_list if data.find(x)!=-1 ]
    if len(ir_url_list)==0: # only 1 experimental data
        ir_url_list = [ (0,fullurl) ]
    # check if digitalized data is available, if yes, get its link
    typed_ir_url_list = []
    for i,url in ir_url_list:
        data = urllib.request.urlopen(url).read().decode("utf-8")
        if data.find('in JCAMP-DX format.')!=-1:
            digitalizedurl = 'http://webbook.nist.gov/cgi/cbook.cgi?JCAMP={}&Index={}&Type=IR'.format(myid,i)
            typed_ir_url_list.append((i,'digitalized',digitalizedurl))
        elif data.find('A digitized version of this spectrum is not currently available.')!=-1:
            # find the line that looks like:
            # View <a href="/cgi/cbook.cgi?Scan=cob2370&amp;Type=IR">scan of original (hardcopy) spectrum</a>.</p>
            lines = data.split('\n')
            line = [l for l in lines if l.find('scan of original (hardcopy) spectrum')!=-1]
            if len(line)>1:
                raise Exception('More than 1 scanned spectrum while geting list of experimental ir data')
            line = line[0].split('"')[1]
            scannedurl = irurl_header + line
            typed_ir_url_list.append((i,'scanned',scannedurl))
        else:
            raise Exception('Unexpected error while geting list of experimental ir data')
    for i,mytype,url in typed_ir_url_list:
        suffix = { 'scanned':'.gif', 'digitalized':'.jdx' }
        filename = mytype + '/' + myid + '-{}'.format(i) + suffix[mytype]
        file_list.append((url,filename))
    print('{} experimental spectrum from {}'.format(len(typed_ir_url_list),myid))
    return file_list

def read_idlistdone():
    donelist = set()
    if not os.path.isfile(intralist_fn):
        return donelist
    with open(intralist_fn) as f:
        for line in f:
            line = line.strip().split()
            if len(line)==2 and line[1] == 'end':
                donelist.add(line[0])
    return donelist

def write_filelist(myid,f):
    # file format:
    # id start
    # url1 filename1
    # url2 filename2
    # ...
    # id end
    file_list = query_filelist(myid)
    f.write('{} start\n'.format(myid))
    for url,fn in file_list:
        f.write('{} {}\n'.format(url,fn))
    f.write('{} end\n'.format(myid))

def remove_interrupted():
    oldlines = []
    if os.path.isfile(intralist_fn):
        with open(intralist_fn) as f:
            oldlines = [l for l in f]
    with open(intralist_fn,'w') as f:
        for i in oldlines:
            f.write(i)

if not check_file_done(intralist_fn):
    idlist = read_idlist()
    donelist = read_idlistdone()
    todolist = idlist - donelist
    remove_interrupted()
    with open(intralist_fn,'a') as f:
        count = 0
        for i in todolist:
            write_filelist(i,f)
            print('progress: {}/{} {}%'.format(count,len(todolist),round(100.0*count/len(todolist),3)))
            count += 1
        f.write('done\n')

# step 4: download all the files
def get_downloaded_list():
    if not os.path.isfile(files_fn):
        with open(files_fn,'w') as f:
            pass
    downloaded_list = []
    with open(files_fn) as f:
        for line in f:
            filename_md5 = tuple(line.strip().split())
            downloaded_list.append(filename_md5)
    print(len(downloaded_list),'already downloaded')
    # verify md5
    verified_list = []
    for fn,expected_md5 in downloaded_list:
        fullfn = outputdir + '/' + fn
        if not os.path.isfile(fullfn):
            print(fn,': file does not exist')
            continue
        with open(fullfn,'rb') as f:
            data = f.read()
            actuall_md5 = hashlib.md5(data).hexdigest()
            if expected_md5 == actuall_md5:
                verified_list.append((fn,expected_md5))
            else:
                print(fn,': md5 mismatch\n')
    print(len(downloaded_list)-len(verified_list),'is broken')
    # rewrite verified_list back to files_fn
    with open(files_fn,'w') as f:
        for fn,md5 in verified_list:
            f.write('{} {}\n'.format(fn,md5))
    return verified_list

def get_full_filelist():
    full_file_list = {}
    with open(intralist_fn) as f:
        for line in f:
            line = line.strip()
            if line != 'done':
                url,fn = line.split()
                if fn in ['start','end']:
                    continue
                else:
                    full_file_list[fn] = url
    return full_file_list

def prepare_dirs():
    dirs = [ 'mol_files','sdf_files','digitalized','scanned' ]
    dirs = [ outputdir+'/'+x for x in dirs ]
    for d in dirs:
        if not os.path.isdir(d):
            os.makedirs(d)
def writedata(fn,data,files_f):
    md5 = hashlib.md5(data).hexdigest()
    with open(outputdir+'/'+fn,'wb') as f:
        f.write(data)
    files_f.write('{} {}\n'.format(fn,md5))

fulllist = get_full_filelist()
downloaded_list = get_downloaded_list()
# calculate list of files not downloaded yet
full_filenames = set(fulllist.keys())
downloaded_filenames = set([x for x,y in downloaded_list])
remaining = full_filenames - downloaded_filenames
# download files
prepare_dirs()
count = 0
with open(files_fn,'a') as f:
    for fn in remaining:
        url = fulllist[fn]
        data = urllib.request.urlopen(url).read()#.decode("utf-8")
        writedata(fn,data,f)
        print('progress: {}%'.format(round(100.0*count/len(remaining)),3),',',
              count,'of',len(remaining),',',fn)
        count += 1
