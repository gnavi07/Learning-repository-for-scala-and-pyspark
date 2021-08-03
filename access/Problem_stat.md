Employee access table have employee id and their access to corresponding environments.

employee_access

|EmployeeID|Development|Integration|Release|Production|
|----------|-----------|-----------|-------|----------|
|1123|Y|Y|N|N|
|1235|Y|Y|Y|N|
|1125|N|N|N|Y|
|5898|Y|N|N|N|
|2225|N|Y|N|N|
|5898|N|N|Y|N|
|7890|N|N|N|Y|

Derive access_information table based on employee_access table
access_information
Expected output

|EmployeeID|Access|
|----------|------|
|1123|Development|
|1123|Integration|
|1235|Development|
|1235|Integration|
|1235|Release|
|1125|Production|
|5898|Development|
|2225|Integration|
|5898|Release|
|7890|Production|
