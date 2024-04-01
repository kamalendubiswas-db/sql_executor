import re

sql = """
-- This is a single-line before CTE
/*
this is multiline before CTE
*/
WITH cte_name AS (
    SELECT id,name FROM users  --inbetween single
)
--inline single
/*
inline
clear
multi
*/
SELECT * FROM cte_name;
"""

# Regular expression pattern for SQL comments
pattern = r'(?:--.*?$|/\*[\s\S]*?\*/)'
# Use re.MULTILINE to match the beginning of each line for single-line comments
comments = re.findall(pattern, sql, re.MULTILINE)

# Print the extracted comments
for comment in comments:
    print(comment)
