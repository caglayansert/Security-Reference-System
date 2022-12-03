from collections import defaultdict
import csv
from data_frame import DataFrame
from datetime import datetime
import os
import pandas as pd

class SecurityData:
	"""
	This class is used for step 4 of the given assignment. It is used to create a security_data.csv file from dataframe
	with the given structure in the assignment.
	"""
	def __init__(self, inputPath, referencePath):
		self.input_path = inputPath
		self.reference_path = referencePath

	def getReferenceFields(self):
		with open(self.reference_path, "r") as file:
				skip_header = next(file)  # skip the heading in csv
				data = list(csv.reader(file, delimiter=","))
		reference_fields = list(d[0] for d in data if d[1] == "1")
		return reference_fields


	def buildSecurityDataFrame(self):
		timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		reference_fields = self.getReferenceFields()
		input_df_obj = DataFrame(self.input_path, self.reference_path)
		limited_columns_input_df = input_df_obj.limitDataFrameColumns(reference_fields)

		x = []
		for i in limited_columns_input_df.index:
			for field in reference_fields:
				if field not in limited_columns_input_df: continue
				if field != "ID_BB_GLOBAL":
					x.append((limited_columns_input_df["ID_BB_GLOBAL"][i], field, limited_columns_input_df[field][i], "corp_pfd.dif", str(timestamp)))


		security_df = pd.DataFrame(x, columns=["ID_BB_GLOBAL", "FIELD", "VALUE", "SOURCE", "TSTAMP"])
		return security_df

	def createSecurityDataFile(self, outputPath):

		security_df = self.buildSecurityDataFrame()
		return security_df.to_csv(outputPath, index=False)


if __name__ == "__main__":

	input_path = os.path.join(os.path.dirname(__file__), "corp_pfd.dif")
	reference_fields_path = os.path.join(os.path.dirname(__file__), "reference_fileds.csv")
	output_path = os.path.join(os.path.dirname(__file__), "security_data.csv")
	obj = SecurityData(inputPath=input_path, referencePath=reference_fields_path)
	print(obj.buildSecurityDataFrame())
	obj.createSecurityDataFile(output_path)

