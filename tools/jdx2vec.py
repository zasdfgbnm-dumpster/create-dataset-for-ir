from array import array

def wl2wn(wl):
    return 10000/wl

def absorb2trans(absorb,yfactor):
    return 10**(yfactor*absorb)

def parse_state(state):
    statestr = state.strip()
    statesplit = statestr.split('(')
    _state1 = statesplit[0]
    statesplit = [''] + statesplit[1:]
    _state1split = _state1.split(' ')
    state1 = _state1split[0].strip().lower()
    state2 = ( ' '.join(_state1split[1:]) + '('.join(statesplit) ).strip()
    # further parse state2
    if len(state2) > 0 and state2[0] == '(' and state2[-1] == ')':
        state2 = state2[1:-1].strip()
    return state1,state2

# convert x,y data to points at standard x
def standard(xyxy):
    xmin = 670
    xmax = 3702
    xstep = 4
    yy = array('f')
    xyxy.sort(reverse=True)

    x = xmin
    low_resolution_count = 0
    xl,yl = xyxy.pop()
    while x<=xmax:
        if len(xyxy) == 0:
            return None
        xr,yr = xyxy[-1]
        if x < xl:
            return None
        elif x == xl:
            yy.append(yl)
            x += xstep
            low_resolution_count += 1
        elif x <= xr:
            yy.append( yl+(yr-yl)/(xr-xl)*(x-xl) )
            x += xstep
            low_resolution_count += 1
        else:
            xl,yl = xyxy.pop()
            if low_resolution_count > 0:
                low_resolution_count -= 1

    if low_resolution_count > 50:
        return None
    else:
        return yy


def jdx2vec(filename):
    # read records and xyy data
    f1 = open(filename)
    record = {}
    xyy = []
    inxyy = False
    record['STATE'] = 'unknown'
    for j in f1:
        if j[0:2] == '##':
            varname = j.split('=')[0][2:].strip()
            varvalue = j.split('=')[1].strip()
            if varname == 'XYDATA' and varvalue == '(X++(Y..Y))':
                inxyy = True
                continue
            if varname == 'END':
                inxyy = False
                continue
            record[varname] = varvalue
        elif inxyy:
            j = j.replace('-',' -',9999)
            numbers = j.split()
            x = float(numbers[0])
            y = [ float(yy) for yy in numbers[1:] ]
            xyy.append((x,y))
    f1.close()
    # further parse state
    state1,state2 = parse_state(record['STATE'])
    # generate [(x,y)..] data, x in 1/CM, y in transmittance
    xyxy = []
    firstx = float(record['FIRSTX'])
    lastx = float(record['LASTX'])
    npoints = int(record['NPOINTS'])
    deltax = (lastx-firstx)/(npoints-1)
    yfactor = float(record['YFACTOR'])
    xunit = record['XUNITS']
    yunit = record['YUNITS']
    for x,yy in xyy:
        for j in range(len(yy)):
            _x = x + deltax*j
            if xunit == 'MICROMETERS':
                _x = 10000/_x
            _y = yy[j]*yfactor
            if yunit == 'ABSORBANCE':
                _y = 10**(-_y)
            xyxy.append((_x,_y))
    return standard(xyxy),state1,state2
