from data_frame import DataFrame
import pandas as pd
import os

class CompareSecurities:
	"""
	This class is used for the step 3 of the given assignment. It is used for creating a new dataframe then a new csv file,
	which will store the securities which are not in the given reference securities file. The structure of the new file
	is matching with the given reference file. Used "ID_BB_GLOBAL" as key.
	"""
	def __init__(self, inputPath, securitiesPath):
		self.input_path = inputPath
		self.securities_path = securitiesPath
	
	def getColumsInReference(self):
		securities_df = pd.read_csv(self.securities_path)
		columns = list(col.upper() for col in securities_df.columns)
		return columns

	def limitDataFrameColumns(self):
		# limit columns to show in dataframe depending on referenceFields
		df = DataFrame(self.input_path, self.securities_path)
		data_frame = df.buildDataFrame()
		_, cols = df.getRowsAndColsForDataFrame()
		reference_fields = self.getColumsInReference()
		data_frame.drop([col for col in cols if col not in reference_fields], inplace=True, axis=1)
		data_frame = data_frame[reference_fields]
		return data_frame

	def getRowsNotInReference(self):

		rows_in_reference = set()  # get ID_BB_GLOBAL as key from each line and store in a set
		with open(self.securities_path, "r") as f:
			lines = f.read().splitlines()
		for line_idx in range(1, len(lines)):
			row = lines[line_idx].split(",")
			rows_in_reference.add(row[0]) # 0 index is the ID_BB_GLOBAL

		df = self.limitDataFrameColumns() # get the data frame limited with same columns in referecen file
		
		input_rows = [] # store all "ID_BB_GLOBAL" data rows in original input data frame 
		for idx in df.index:
			input_rows.append(df["ID_BB_GLOBAL"][idx])

		# it seems there is only one line not in the reference securities, but there many securities in reference file but not in input file.
		new_rows = [] # store rows not in the reference securities but in input data.
		for row in input_rows:
			if row not in rows_in_reference:
				new_rows.append(row)

		for idx in df.index:
			if df["ID_BB_GLOBAL"][idx] not in new_rows:
				df.drop(idx, inplace=True) # remove rows not in new_rows to build new data frame for new_securities
		return df


	def createNewSecurityFile(self, outputPath):

		security_df = self.getRowsNotInReference()
		return security_df.to_csv(outputPath, index=False)


if __name__ == "__main__":

	input_path = os.path.join(os.path.dirname(__file__), "corp_pfd.dif")
	securities_path = os.path.join(os.path.dirname(__file__), "reference_securities.csv")
	output_path = os.path.join(os.path.dirname(__file__), "new_securities.csv")
	obj = CompareSecurities(inputPath=input_path, securitiesPath=securities_path)
	print(obj.getRowsNotInReference())
	obj.createNewSecurityFile(output_path)

