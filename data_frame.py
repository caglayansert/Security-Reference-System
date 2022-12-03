import csv
import os
import pandas as pd

class DataFrame:
	"""
	This class get input -> "corp_pfd.dif" and reference -> "reference_fileds.csv" file and build a dataframe 
	by using input data file. Also, limit the dataframe columns with those only found in reference file. This class handles
	steps 1 and 3 given in the assignment.
	Some methods in this class used in for another classes for steps 3 and 4.
	"""
	def __init__(self, input_path, referencePath):
		self.input_path = input_path
		self.reference_path = referencePath

	def getReferenceFields(self):
		""""
		I took the fields to use from the file if their id_field is only 1.
		There is only 1 field in the reference file not in the input data file.
		"""
		with open(self.reference_path, "r") as file:
			skip_header = next(file) #skip the heading in csv
			data = list(csv.reader(file, delimiter=","))
		# store fields are going to be used in a set data structure for further compare it with the input file in O(1) time.
		reference_field_set = set(d[0] for d in data if d[1] == "1")
		return reference_field_set

	def getRowsAndColsForDataFrame(self):
		"""
		The function name is self explanatory, however for further explanation, we are returning two arrays of cols and rows
		to be used in creating the data frame, this is an helper function for creating data frame.
		- columns between lines 6 - 236 inclusive
		- data between lines 241 - 3136 inclusive
		"""
		rows, cols = [], []
		with open(self.input_path, 'r') as f:
			lines = f.read().splitlines() # splitlines() remove "\n" at the end of each line
		for line_idx in range(6, 237):
			col = lines[line_idx]
			# some lines are seperated with headers start with "#" and there are some spaces between lines we don't need in our dataframe.
			if col == "" or col.startswith("#"): continue 
			cols.append(col)
		for line_idx in range(241, 3137):
			# rows are seperated with "|", so we need to get rid of them
			row = lines[line_idx].split("|") # remove "|" in between cols
			# not sure why but, got a space at the end of every row, so I need to get rid of it.
			row.pop() # remove last space from each data
			rows.append(row)
		return rows, cols

	def buildDataFrame(self):
		rows, cols = self.getRowsAndColsForDataFrame()
		# build dataframe
		data_frame = pd.DataFrame((rows[i] for i in range(len(rows))), columns=cols)
		return data_frame

	def limitDataFrameColumns(self, referenceFields):
		"""
		limit columns to show in dataframe depending on referenceFields
		"""
		data_frame = self.buildDataFrame()
		_, cols = self.getRowsAndColsForDataFrame()
		data_frame.drop([col for col in cols if col not in referenceFields], inplace=True, axis=1)
		return data_frame


if __name__ == "__main__":
	input_path = os.path.join(os.path.dirname(__file__), "corp_pfd.dif")
	reference_fields_path = os.path.join(os.path.dirname(__file__), "reference_fileds.csv")

	data_frame_object = DataFrame(input_path, reference_fields_path)
	reference_fields = data_frame_object.getReferenceFields()

	# print(reference_fields)
	print(data_frame_object.buildDataFrame())
	print(data_frame_object.limitDataFrameColumns(reference_fields))
