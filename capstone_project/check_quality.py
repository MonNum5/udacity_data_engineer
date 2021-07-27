# Test for existing rows

def check_row_number(cur, tables):
    """
    Check number of rows greater one for sql tables 
    
    Args:
        cur (object): PostgreSQL curser object
        tables (list): List of string of table names
    """
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")

        if cur.rowcount < 1:
            print(f"ERROR table {table} is empty")
        else:
            print(f"Quality check for {table} successfull")
            
            
def check_column_names(cur, tables, column_dict):
    """
    Check column names of sql tables
    
    Args:
        cur (object): PostgreSQL curser object
        tables (list): List of string of table names
        column_dict (dict): Dict table names and list of corresponding column names
    """
    
    
    for table in tables:
        cur.execute(f"SELECT * FROM {table} LIMIT 5")
        colnames = [desc[0] for desc in cur.description]
        
        if colnames != column_dict[table]:
            print(f"Column names of {table} do not correspond to the ones in column_dict")
            
        else:
            print(f"Successfal column names test for {table}")
            
            
def check_dtypes(cur, tables, dtype_dict):
    """
    Check column names of sql tables
    
    Args:
        cur (object): PostgreSQL curser object
        tables (list): List of string of table names
        dtype_dict (dict): Dict table names and list of corresponding dtypes of columns
    """
    
    for table in tables:
        cur.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
        dtype = [_[0] for _ in cur]

        if dtype != dtype_dict[table]:
            print(f"Data types of {table} do not correspond to the ones in dtype_dict")
            
        else:
            print(f"Successfal data types test for {table}")
            
        