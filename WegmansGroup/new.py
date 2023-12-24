
from datetime import datetime

date_string = "07/30/2022"
date_format = "%m/%d/%Y"
print("date_string =", date_string)
print("type of date_string =", type(date_string))

date_object = datetime.strptime(date_string, date_format)

print("date_object =", date_object)
print("type of date_object =", type(date_object))

















