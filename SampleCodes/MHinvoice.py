import os
import sys
os.environ['PYTHON_EGG_CACHE'] = os.getcwd()
import MySQLdb
import xlrd
import datetime
conn=MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs =conn.cursor()

def MHFileLoad(MH_file):
#	workbook = xlrd.open_workbook(MH_file)
#	worksheet = workbook.sheet_by_name('Sheet1')
#	for curr_row in range(1,worksheet.nrows):
#		row = worksheet.row(curr_row)
#		print row[0]
#		if row[0] == 'empty:''':
#			print "Last Line out of the file"
	workbook = xlrd.open_workbook(MH_file)
	worksheet = workbook.sheet_by_name('Sheet1')
	num_rows = worksheet.nrows - 1
	num_cells = worksheet.ncols - 1
	curr_row = -1
	while curr_row < num_rows:
		curr_row += 1
		row = worksheet.row(curr_row)
		print 'Row:', curr_row
		curr_cell = -1
		values = []
		while curr_cell < num_cells:
			curr_cell += 1
			# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
			cell_type = worksheet.cell_type(curr_row, curr_cell)
			cell_value = worksheet.cell_value(curr_row, curr_cell)
#			print '	', cell_type, ':', cell_value
			if cell_type == xlrd.XL_CELL_DATE:
#				value = xlrd.xldate_as_tuple(worksheet.cell_value(row, col), workbook.datemode)
#				value	xlrd.xldate_as_tuple(worksheet.row(rownum)[colnum].value, workbook.datemode)
			elif cell_type == xlrd.XL_CELL_BOOLEAN:
#				value = bool(worksheet.cell_value(row, col))
			values.append(cell_value)
			if cell_value == '':
				print "Last Line out of the file"
				return 1
		print values
def getdate(dt):
        if 'float' in str(type(dt)):
                return  str(datetime.datetime(1899,12,30)+datetime.timedelta(dt)).split()[0]
        char=''
        if dt.find('/'):
                char='/'
        elif dt.find('-'):
                char='-'
        d= [ int(d) for d in dt.split(char)]
        return str(datetime.date(d[2],d[0],d[1]))


if __name__ =='__main__':
	MH_file=raw_input("enter the file name with full path:")
	MHFileLoad(MH_file)
	print "File Fully readed"
