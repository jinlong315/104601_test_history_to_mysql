from data_cleaning.Module_fat_internal_report import GetFileList
from data_cleaning.Module_fat_internal_report import FATInternalReport
from data_cleaning.Module_fat_internal_report import StackNumber
from data_cleaning.Module_fat_internal_report import failed_itmes
import pandas as pd
from sqlalchemy import create_engine


# define variables to save directory
dir_104601 = r"D:\ShareCache\上海韵量新能源科技有限公司\产品技术部PTD\PTD China\Testing\4. Production FAT Reports\Electra 1.0 211E with Dummy cells\2. FAT internal reports"
# call function to get the full name of file in the directory
list_csv_fullname = GetFileList(dir_104601).get_csv_files()

# call function to get the cleaned data
test_history = pd.DataFrame()
for i in list_csv_fullname:
    try:
        data = FATInternalReport(csv_directory=i).test_history()
        test_history = pd.concat([data, test_history], axis=0)
    except:
        print("failed_file: " + i.split("\\")[-1])

# reset index of DataFrame
test_history.reset_index(inplace=True)
# drop original index
test_history.drop(columns=["index"], inplace=True)

# call function to clean data
new_stack_number = []
stack_category = []

# ergodic index of DataFrame
for i in test_history.index.to_list():
    # create one instance based on object
    stack_information = StackNumber(part_number="104601",
                                    old_stack_number=test_history.iloc[i]["stack_number"],
                                    cell_count=test_history.iloc[i]["cell_count"])

    # call function to clean data
    new_stack = stack_information.new_stack_number()
    tag = stack_information.stack_category()

    # append to list
    new_stack_number.append(new_stack)
    stack_category.append(tag)

# update DataFrame with cleaned data
test_history["stack_number"] = new_stack_number
test_history["stack_category"] = stack_category
test_history["part_number"] = "104601"

# transfer string to datetime
test_history["test_date_time"] = pd.to_datetime(test_history["test_date_time"])

# create new DataFrame
dim_test_history = test_history.iloc[:][["stack_number", "test_date_time"]]
dim_test_history.drop_duplicates(inplace=True)

# create new column for newly created DataFrame
dim_test_history["test_times"] = dim_test_history.groupby("stack_number")["test_date_time"].rank(method="min", ascending=True)

# concatenate DataFrame
df_merged = pd.merge(left=test_history,
                     right=dim_test_history[["test_date_time", "test_times"]],
                     left_on="test_date_time",
                     right_on="test_date_time")

df_failed_items = failed_itmes()
df_merged = pd.merge(left=df_merged,
                     right=df_failed_items,
                     left_on="failed_items",
                     right_on="failed_items",
                     how="left")

# sort values
df_merged.sort_values(by=["test_date_time"], ascending=True, inplace=True)

# create connection to MySQL
connection = create_engine("mysql+pymysql://root:Hjl19910615@localhost:3306/process_quality")
if connection:
    print("connection successfully !!")

# data save into MySQl
df_merged.to_sql(name="104601_fat_test_history",
                 con=connection,
                 if_exists="replace",
                 index=False)
