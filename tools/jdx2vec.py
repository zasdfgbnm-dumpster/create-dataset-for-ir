def wl2wn(wl):
    return 10000/wl

def absorb2trans(absorb,yfactor):
    return 10**(yfactor*absorb)

# convert x,y data to points at standard x
def standard(xyxy):
    xmin = 670
    xmax = 3702
    xstep = 4
    yy = []
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
            yy.push(yl)
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
	for j in f1:
		if j[0:2] == '##':
			varname = j.split('=')[0][2:].strip()
			varvalue = j.split('=')[1].strip()
			record[varname] = varvalue
		elif j[0].isdigit():
			numbers = j.split()
			x = float(numbers[0])
			y = [ float(yy) for yy in numbers[1:] ]
			xyy.append((x,y))
	f1.close()
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
    return standard(xyxy),record['STATE']
