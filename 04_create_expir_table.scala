import scala.math._
import sparkSession.implicits._
import org.apache.spark.sql._
import org.apache.spark._
import scala.collection.mutable._

def wl2wn(wl:Float):Float = 10000 / wl

def absorb2trans(absorb:Float,yfactor:Float):Float = pow(10, yfactor*absorb)

def parse_state(state:String):(String,String) = {
    val statesplit = state.trim().split(raw"\s+",2).map(_.trim())
    (statesplit[0],statesplit[1])
}

// convert x,y data to points at standard x
//TODO: How to let PriorityQueue order ascending?
def standard(xyxy:PriorityQueue[(Float,Float)]):Array[Float] = {
    val xmin = 670 //included
    val xmax = 3702 //included
    val xstep = 4
    val yy = Array[Float]( (xmax-xmin)/xstep + 1 )

    var x = xmin
    var low_resolution_count = 0
    var (xl,yl) = xyxy.dequeue()
	var idx = 0
    while(x <= xmax){
        val (xr,yr) = xyxy.head
        if(x < xl)
            return None //TODO: how to replace the return with scala correspondings?
        else if(x <= xr) {
            yy(idx) = if(x==xl) yl else yl+(yr-yl)/(xr-xl)*(x-xl)
            x += xstep
			idx += 1
            low_resolution_count += 1
        } else {
            xl,yl = xyxy.dequeue()
            if(low_resolution_count > 0)
                low_resolution_count -= 1
		}
	}

    if(low_resolution_count > 50)
        return None //TODO: how to replace the return with scala correspondings?
    yy
}

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


# now start
from os import listdir
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext(appName="03_create_expir_table")

# read junk list
fnjunk = '/home/gaoxiang/create-dataset-for-ir/outputs/02/all.junk.smi'
junklist = [ i.split()[1] for i in open(fnjunk) ]

# process jdx files
inputdir = '/home/gaoxiang/create-dataset-for-ir/outputs/01/digitalized/'

def process(i):
    # get mid and index
    ii = i.split('.')[0].split('-')
    mid = ii[0]
    index = int(ii[1])
    # skip junk
    if mid in junklist:
        return None
    vec,state,state_info = jdx2vec(inputdir+i)
    # skip non-standardizable data
    if vec is None:
        return None
    row = Row(mid=mid,index=index,vec=vec,state=state,state_info=state_info)
    return row

filelist = sc.parallelize(listdir(inputdir),200)
rows = filelist.map(process).filter(lambda i: i is not None)

# generate table
sqlContext = SQLContext(sc)
expir = sqlContext.createDataFrame(rows)
expir.show()
expir.write.parquet('outputs/03/expir')
print('done')
