
import pydata_google_auth
import os
import time
import getpass
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.exceptions import NotFound
from typing import List, Union

class HumanBytes:
    """Outer class structure for properties needed to create reliable human-readable memory size formats
    This should ONLY be used as HumanBytes.format(###,kwargs). HumanBytes doesn't need to be instantiated
    format is a staticmethod"""
    #NOTE: Not created within the team but found and kept as from Stack Overflow solution, link:
    #https://stackoverflow.com/questions/12523586/python-format-size-application-converting-b-to-kb-mb-gb-tb
    
    METRIC_LABELS: List[str] = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    BINARY_LABELS: List[str] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
    PRECISION_OFFSETS: List[float] = [0.5, 0.05, 0.005, 0.0005] # PREDEFINED FOR SPEED.
    PRECISION_FORMATS: List[str] = ["{}{:.0f} {}", "{}{:.1f} {}", "{}{:.2f} {}", "{}{:.3f} {}"] # PREDEFINED FOR SPEED.

    @staticmethod
    def format(num: Union[int, float], metric: bool=False, precision: int=1) -> str:
        """
        Human-readable formatting of bytes, using binary (powers of 1024)
        or metric (powers of 1000) representation.
        """

        assert isinstance(num, (int, float)), "num must be an int or float"
        assert isinstance(metric, bool), "metric must be a bool"
        assert isinstance(precision, int) and precision >= 0 and precision <= 3, "precision must be an int (range 0-3)"

        unit_labels = HumanBytes.METRIC_LABELS if metric else HumanBytes.BINARY_LABELS
        last_label = unit_labels[-1]
        unit_step = 1000 if metric else 1024
        unit_step_thresh = unit_step - HumanBytes.PRECISION_OFFSETS[precision]

        is_negative = num < 0
        if is_negative: # Faster than ternary assignment or always running abs().
            num = abs(num)

        for unit in unit_labels:
            if num < unit_step_thresh:
                # VERY IMPORTANT:
                # Only accepts the CURRENT unit if we're BELOW the threshold where
                # float rounding behavior would place us into the NEXT unit: F.ex.
                # when rounding a float to 1 decimal, any number ">= 1023.95" will
                # be rounded to "1024.0". Obviously we don't want ugly output such
                # as "1024.0 KiB", since the proper term for that is "1.0 MiB".
                break
            if unit != last_label:
                # We only shrink the number if we HAVEN'T reached the last unit.
                # NOTE: These looped divisions accumulate floating point rounding
                # errors, but each new division pushes the rounding errors further
                # and further down in the decimals, so it doesn't matter at all.
                num /= unit_step

        return HumanBytes.PRECISION_FORMATS[precision].format("-" if is_negative else "", num, unit)


def format_XLS(xlsxName,ListoFLists_DFandSheetName):
    """Write workbook sheet by sheet with pd.ExcelWriter just nicer formatting with 
     wraps text on headers, drops index, needs list of lists with outer list being number of sheets to 
     write, each having an inner list of 2 items, the DF to source from and the sheetname to write to.
     AS BQ timezones in a timestamp Barf excel, removes zone from timestamps
     will add .xlsx to end of any files that don't already have this extension"""
    xlsxName=xlsxName.strip()#Remove leading/trailing blanks in name
    if xlsxName[-5:]!='.xlsx':#Add extension if not sent
        xlsxName=xlsxName+'.xlsx'
    #Common miscall when used with a single sheet is to forget outer list, include exception handling
    #so if there is no inner list in ListoFLists_DFandSheetName, it is wrapped in a second list.
    if type(ListoFLists_DFandSheetName[0])!=list:
        ListoFLists_DFandSheetName=[ListoFLists_DFandSheetName]
        
    
    xlsxwriter = pd.ExcelWriter(xlsxName, engine='xlsxwriter')
    
    for listicle in ListoFLists_DFandSheetName:
        df=listicle[0]
        SheetName=listicle[1]
        
        for x in df.columns:#strip out timezones so excel doesn't barf (no timezones in excel)
            if 'datetime64' in str(df[x].dtype):
                df[x]=df[x].dt.tz_localize(None) 
        
        df.to_excel(xlsxwriter, sheet_name=SheetName, startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        workbook  = xlsxwriter.book
        worksheet = xlsxwriter.sheets[SheetName]

        # Add a header format.
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1})

        # Write the column headers with the defined format.
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            
        worksheet.autofilter(0, 0, df.shape[0], df.shape[1])
            
    # Close the Pandas Excel writer and output the Excel file.
    xlsxwriter.save()

def list_Ram(n=10,HumanReadable=True):
    """List the top n objects using memory in the current kernal in descending order by memory required
    HumanReadable=True will format into Gib, MIb etc. for easier consumption"""
    import pandas as pd
    import sys

    all_obj =globals()

    object_name = list(all_obj).copy()
    object_size = [sys.getsizeof(all_obj[x]) for x in object_name]

    d = pd.DataFrame(dict(name = object_name, size = object_size))
    d.sort_values(['size'], ascending=[0],inplace=True)
    if HumanReadable:
        d['size']=d['size'].apply(HumanBytes.format)

    return(d.head(n))

class BQ_User():
    """Class for easier interfacing with Bigquery, implements BQ client and groups useful methods under:
        .SQL: Run a SQL statement, possibly returning as a pandas Dataframe, also has canned implementation
                of THD Price Ending logic for pricing team
        .Table: Additional table specific function for reveiwing or managing tables
        .Dataset: tools for management of Dataset level objects
        .test: Single function to run basic counts or sums to show tests during development"""
    
    
    #DEFITIONS: BQ_USER direct Methods
    def __init__(self,dataset = None,project_name='analytics-pricing-thd'):
        self.project_name= project_name
        if dataset:
            self.dataset = dataset
        else:
            # Generate default dataset using our best guess
            self.dataset = 'USER_'+getpass.getuser().upper()
        
        self.credentials = pydata_google_auth.get_user_credentials(
            ['https://www.googleapis.com/auth/bigquery'],
        )
        self.client = bigquery.Client(project=self.project_name, credentials=self.credentials)
        try:
            # Attempt to instantiate a bigquery storage client, can make to_df faster
            self.storageClient = bigquery_storage.BigQueryReadClient(credentials=self.credentials)
        except:
            pass
        
        # Instantiate subclasses
        # These implement various BQ_User methods, split into subclasses for organization
        self.Dataset = self.__Dataset(self)
        self.SQL = self.__SQL(self)
        self.Table = self.__Table(self)
    
    
    def test(self,CTEorTable,CountDFields=False,SumFields=False,PriorCTECode=False):
        """Assist function to examine results of a CTE or table by printing to the console the results of
        count Obs and any requested Distinct Counts or Sums, Distinct Count strings can have , for concat
        Default value false disables a trait
        CTEorTable : The table or CTE to examine and return results from, CTEs should be inside parentheses
        CountDFields=False : Field sets as strings to return Count Distinct on, delimite multiple fields with ','
                    Returns fields named CntDst_XXX, with X determined by Input. Inputs can be:
                    single string - Single field uses Name taken from String, multiple fields use Field0
                    List/Tuple of strings - Will do Count Distinct for each, field names as above, Field# increments by 1
                    List/Tuple of length 2 inner List/Tuple - First value is field(s), Second is Name for that count
        SumFields=False : Significant feilds to return a sum on, can be single string or list of strings
        PriorCTECode=False : For a CTE that uses a prior CTE, pass string of preceeding CTEs here

        """

        if PriorCTECode: #Change select to include prior CTE if used with CTE creation
            SQL=PriorCTECode+"""
            select cast(count(*) as numeric) as Num_Obs"""
        else:
            SQL='select cast(count(*) as numeric) as Num_Obs'
        if CountDFields:
            CountDcnt=0 #Name counter for multiple field concats
            if type(CountDFields) not in (list, tuple): #Explicit list handling for for loop
                CountDFields=[CountDFields]

            for Field in CountDFields:
                FieldValue=Field#Original Style, Stays in place if Field is not an inner list with a name
                if type(Field) in (list, tuple): #Explicit Names from inner list or tuple
                    FieldName=Field[1]
                    FieldValue=Field[0]
                elif ',' in Field:#If multiple values in Distinct, Number the fields as a default
                    FieldName='Field'+str(CountDcnt)
                else:#SIngle field can use it's own name as the default name for a single fiel
                    FieldName=Field

                SQL=SQL+', cast(count(distinct CONCAT('+FieldValue+')) as numeric) as CntDst_'+FieldName
                CountDcnt+=1

        if SumFields:
            if type(SumFields) not in (list, tuple): #Explicit list handling for for loop
                SumFields=[SumFields]

            for Field in SumFields:#Sum is always a single field
                SQL=SQL+', FORMAT("%.2f",cast(sum('+Field+') as NUMERIC)) as Sum_'+ Field
        
        SQL=SQL+' from '+CTEorTable+';'
        df=self.client.query(SQL).result().to_dataframe()
        for col in df.columns.to_list(): #Coerce fields to numeric
            df[col]=pd.to_numeric(df[col],errors='coerce')
        display(df.style.format('{:,}'))#return display results with , for large numbers
    
    def __nameDeparse(self,fullTable,dataset,project_name):
        """Internal convienence function that allows certain functions that take separate 
        table name, dataset, project to also handle pasting in the BQ `proj.dataset.table`
        or BQ dataset.table format with default project
        format as a single string
        Also handles if None then default dataset and project that is common behavior"""
        
        fullTable=fullTable.replace(':','.')#Allows colons to be interpreted since BQ shows them like this
        #avoids common C&P error
        if '.' in fullTable:
            fullTablePath=fullTable.replace(r'`','').split('.')
            tablename=fullTablePath[-1]
            dataset=fullTablePath[-2]    
            if len(fullTablePath)==3:
                project_name=fullTablePath[0]
            else:
                project_name=None
        else:
            tablename=fullTable
             
        if project_name is None:
            project_name = self.project_name
        if dataset is None:
            dataset = self.dataset 
            
        return(project_name,dataset,tablename)    
    
    #DEFITIONS: Inner Classes inside BQ User

    class __Dataset(object):
        '''
        Tools for management of Dataset level objects
        .create: Create new datasets
        .drop: Delete existing datasets
        '''
         #DEFITIONS: BQ_USER direct Methods

        def __init__(self,bqInstance):
            '''
            Subclass of BQ_User, initialized by BQ_User instance.
            bqInstance: an already instantiated BQ_User object, holds metadata used by Dataset subclass
            '''
            self.__bq = bqInstance

        def create(self,datasetName):
            '''
            Creates a dataset in BQ if it does not already exist.
            Dataset will be created in self.project_name (analytics-pricing-thd by default)
            '''
            try:
                self.__bq.client.get_dataset(datasetName)
                print(f'Dataset {self.__bq.client.project}.{datasetName} already exists')
            except NotFound:
                print(f'Dataset {self.__bq.client.project}.{datasetName} not found, creating it now...')
                newDataset = bigquery.Dataset(f'{self.__bq.client.project}.{datasetName}')
                newDataset = self.__bq.client.create_dataset(newDataset,timeout=30)
                print(f'Dataset {self.__bq.client.project}.{datasetName} successfully created')

        def drop(self,datasetName,delete_contents=False):
            '''
            Deletes a dataset in BQ if it exists, print an error otherwise
            delete_contents is a boolean that defaults to False, if dataset contains tables and this is False,
                the request will fail. Set to True to delete all tables in dataset
            '''
            try:
                self.__bq.client.get_dataset(datasetName)
                print(f'Deleting dataset {self.__bq.client.project}.{datasetName}...')
                datasetToDelete = bigquery.Dataset(f'{self.__bq.client.project}.{datasetName}')
                self.__bq.client.delete_dataset(datasetToDelete,delete_contents=delete_contents,timeout=30
                                                , not_found_ok=False)
                print(f'Dataset {self.__bq.client.project}.{datasetName} deleted!')
            except NotFound:
                print(f'Dataset {self.__bq.client.project}.{datasetName} does not exist, cannot delete!')
        
    class __SQL(object):
        '''
        Core SQL methods to execute as SQL statements on BQ
            .run: run the code on the server as is
            .to_df: run a select statement returning the results as a pandas dataframe. If returning a
                    complete table as is, Table.to_df may be more effecient
            .price_ending: canned pricing team logic to add Price Endings to a table
        '''

        def __init__(self,bqInstance):
            '''
            Subclass of BQ_User, initialized by BQ_User instance.
            bqInstance: an already instantiated BQ_User object, holds metadata used by SQL subclass
            '''
            self.__bq = bqInstance

        def run(self,sql_query,displayrows=True):
            """Runs a prepared SQL statement as is, if displayrows is True, will attempt to print result rows
                A list or tuple of queries may also be provided. In this case the list will run sequentially"""
            if type(sql_query) not in (list, tuple):
                sql_query=[sql_query]
            start,query_count = time.time(),1 #Print Statement inputs, timer and # for multiple query run
            for query in sql_query:
                query_job = self.__bq.client.query(query, job_config = bigquery.QueryJobConfig())
                res = query_job.result()
                if len(sql_query)>1:
                    print('After Query #:',query_count,' SQL Statements run in: ', time.time() - start)
                    query_count+=1
                else:
                    print('SQL Statement run in: ', time.time() - start)
                if displayrows:
                    for row in res:
                        print(row.values())

        def to_df(self,sql_select):
            """Returns a dataframe by running a SQl statement. Note: if copying a single entire table
            the Table_DF function is faster (Table_DF uses BQ Storage API instead of sql)"""
            return self.__bq.client.query(sql_select).result().to_dataframe()

        def price_ending(self,BQTableName,PriceColumnName,RoundDown=True, RoundUp=True):
            '''
            Add column(s) with THD Price ending logic to a specified column of prices in a BQ Table.
            Returns nothing as columns are added to the existing table
            BQTableName=Full table name as `ProjectName.Dataset.TableName` of BQ table to append rounded price columns to.
                This MUST run on a BQ Table, as it leverages other BQ data to lookup proper price intervals
            PriceColumnName=String column name on FullBQTableName to get original unrounded prices from.
            RoundDown=True, This is typical rule for rounding at THD, if True generates a column on FullBQTableName
                with the name RND_DOWN_PriceColumnName, rounding to price ending <= Original value
                will overwrite an existing RND_DOWN_PriceColumnName column
            RoundUp=True, Usually as option to compare to more typical RoundDown, if True generates a column 
                on FullBQTableName with the name RND_DOWN_PriceColumnName, rounding to price ending >= Original value            
            '''        

            if RoundDown==False and RoundUp==False:
                print('No Rounding, needs min. one of RoundDown=True, RoundUp=True')
                return

            #Header Overwrite original name
            SQL = f'''
                CREATE OR REPLACE TABLE {BQTableName} AS'''
            #Under $100 Price Ending Source Table from BQ links to RoundDown by default. CTE to get next larger value
            if RoundUp:
                SQL = SQL+f'''
                    With RoundUpCTE AS (SELECT {PriceColumnName}, MIN(NEW_RETL_PPS) AS NEW_RETL_PPS_UP
                        FROM {BQTableName} a
                        join `analytics-pricing-thd.LAB_QH.PPS_LOGIC` b
                        on a.{PriceColumnName}<=b.NEW_RETL_PPS
                        GROUP BY {PriceColumnName}
                    )'''

            #Logic to overwrite existing column name if a duplicate is found
            testcols=self.to_df(f'''Select * from {BQTableName} limit 5''')
            ExceptStatement=''
            if ('RND_DOWN_'+PriceColumnName in testcols.columns) and ('RND_UP_'+PriceColumnName in testcols.columns):
                ExceptStatement = f'''except (RND_DOWN_{PriceColumnName},RND_UP_{PriceColumnName})'''
            elif 'RND_DOWN_'+PriceColumnName in testcols.columns:
                ExceptStatement = f'''except (RND_DOWN_{PriceColumnName})'''            
            elif 'RND_UP_'+PriceColumnName in testcols.columns:
                ExceptStatement = f'''except (RND_UP_{PriceColumnName})'''

            SQL = SQL+f'''
                    SELECT inputTable.* '''+ExceptStatement


            if RoundDown: #Column name and original over $100 logic for rounding DOWN
                SQL = SQL+f'''        
                    ,CASE 
                      WHEN ROUND((inputTable.{PriceColumnName}),2)>100 AND MOD(CAST(FLOOR(ROUND((inputTable.{PriceColumnName}),2)) AS INT64),10) < 4 THEN FLOOR(inputTable.{PriceColumnName})-(MOD(CAST(FLOOR((inputTable.{PriceColumnName})) AS INT64),10)+1)
                      WHEN ROUND((inputTable.{PriceColumnName}),2)>100 AND MOD(CAST(FLOOR(ROUND((inputTable.{PriceColumnName}),2)) AS INT64),10) >= 4 THEN FLOOR(inputTable.{PriceColumnName})
                      WHEN ROUND((inputTable.{PriceColumnName}),2)<100 THEN PRM_DOWN.NEW_RETL_PPS
                      ELSE inputTable.{PriceColumnName}
                    END AS RND_DOWN_'''+PriceColumnName
            if RoundUp: #Column name and modified over $100 logic for rounding UP
                SQL = SQL+f'''
                    ,CASE 
                      WHEN ROUND((inputTable.{PriceColumnName}),2)>100 AND MOD(CAST(CEILING(ROUND((inputTable.{PriceColumnName}),2)) AS INT64),10) < 4 THEN CEILING(inputTable.{PriceColumnName})+(4-MOD(CAST(CEILING((inputTable.{PriceColumnName})) AS INT64),10))
                      WHEN ROUND((inputTable.{PriceColumnName}),2)>100 AND MOD(CAST(CEILING(ROUND((inputTable.{PriceColumnName}),2)) AS INT64),10) >= 4 THEN CEILING(inputTable.{PriceColumnName})
                      WHEN ROUND((inputTable.{PriceColumnName}),2)<100 THEN RoundUpCTE.NEW_RETL_PPS_UP
                      ELSE inputTable.{PriceColumnName}
                    END AS RND_UP_'''+PriceColumnName

            SQL = SQL+f'''
                    FROM {BQTableName} inputTable '''

            if RoundDown: #join for under $100, matches to penny and by default gives next lower valid price ending
                SQL = SQL+f''' 
                    LEFT JOIN `analytics-pricing-thd.LAB_QH.PPS_LOGIC` PRM_DOWN
                    ON
                    ROUND({PriceColumnName},2)=PRM_DOWN.OLD_PPS'''

            if RoundUp: #Join to CTE for next higher price over $100, bc $100 is equal, always works for $100
                SQL = SQL+f'''            
                    LEFT JOIN RoundUpCTE
                    ON inputTable.{PriceColumnName} = RoundUpCTE.{PriceColumnName}'''

            self.run(SQL,displayrows=False)
            return
        
        def create_proc(self,sql_query, process_name, req_args="",dataset=None, project_name=None):
            """Create a BigQuery stored procedure process_name
            req_args="" string for procedure arguments if any ("" results in no arguments required) 
                stored procedure arguments must be a single string of comma delimited name type 
                example: 'x INT64, y INT64'            
            Once created, stored procedures can be run in SQL using CALL `project.dataset.name`(args values)
            To delete a stored procedure, use the syntax DROP PROCEDURE `project.dataset.name`
            SQL.run can be used to execute both CALL and DROP statements
            When project_name/dataset for the table is None, assumes class defaults 
            Note: If process_name is delimited by '.' will attempt to parse this to get dataset, process_name
            (and project_name if present), can also receive seperate fields for 
                    dataset and project_name
            """
            project_name,dataset,process_name=self.__bq._BQ_User__nameDeparse(process_name,dataset,project_name)             
        
            self.run(f"""CREATE OR REPLACE PROCEDURE 
                `{project_name}.{dataset}.{process_name}`({req_args})
                BEGIN
                {sql_query}
                END;""")
            
            print(f'Stored procedure `{project_name}.{dataset}.{process_name}` successfully created!')

    class __Table(object):
        '''
        Methods to easily interact with SQL table in BQ
            .from_SQL: Creates a table using a SQL Statement, adds table wrapper with optional temp table logic
            .drop: Drop an existing table on the SQL Server
            .to_df: downloads an entire SQL table as is to a local pandas dataframe
            .upload: upload a dataframe or file to a new SQL table 
            .quick_view: Check a table from Python, returns rw count and a pandas dataframe of the top 100 rows
            .detail_view: Implements multiple additional tests to check column wise nulls, mins, max and other
                    statistics. Saves results to a local .xlsx file.
        '''

        def __init__(self,bqInstance):
            '''
            Subclass of BQ_User, initialized by BQ_User instance.
            bqInstance: an already instantiated BQ_User object, holds metadata used by Table subclass
            '''
            self.__bq = bqInstance

        def from_SQL(self,sql_query, table_name = 'temp',dataset=None,project_name=None,expires=True):
            """Runs a SQL statement that must be provided and stores the result in a table
                by default this table is named temp, uses class default dataset and project_name and 
                    self destructs after 7 hours,
                expires=False disables self destruct
                Note: If table_name is provided and delimited by '.' will attempt to parse this to get dataset, 
                    table_name (and project_name if present), can also receive seperate fields for 
                    dataset and project_name
                """
            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name)      

            optString=""
            if expires:
                optString="OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP, INTERVAL 7 HOUR))"

            query = f"""
                CREATE OR REPLACE TABLE `{project_name}.{dataset}.{table_name}`
                    {optString}
                AS
                {sql_query}
            """

            self.__bq.SQL.run(query, False)

            print(f'Table created in: {table_name}: ', time.time() - start)


        def drop(self,table_name,dataset=None,project_name=None):
            """Quick table drop
            If project_name/dataset for the table is None, assumes class defaults 
            Note: If table_name is delimited by '.' will attempt to parse this to get dataset, table_name
            (and project_name if present), can also receive seperate fields for 
                    dataset and project_name
            """
            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name) 

            self.__bq.SQL.run(f'''DROP TABLE `{project_name}.{dataset}.{table_name}` ''', False)



        def to_df(self,table_name,dataset=None,project_name=None):
            """Uses the BQ storage API to download an entire table, for large tables, significantly faster
            Uses default project_name and dataset when these values are None
            Note: If table_name is delimited by '.' will attempt to parse this to get dataset, table_name
            (and project_name if present), can also receive seperate fields for 
                    dataset and project_name"""

            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name) 

            try:#Try except as this sdk seems to have especially bad versioning error issues
                table = f'projects/{project_name}/datasets/{dataset}/tables/{table_name}'        

                requested_session = bigquery_storage.types.ReadSession(
                    table=table,
                    # Need to define data format, Apache ARROW is a format for serializing data
                    # https://arrow.apache.org/docs/python/install.html
                    data_format=bigquery_storage.types.DataFormat.ARROW
                )
                session = self.__bq.storageClient.create_read_session(
                    parent=f'projects/{self.__bq.project_name}', # Billing project   TODO FIX ATTRIBUTE
                    read_session=requested_session
                )

                # The session can generate multiple streams based on the size of the data, we want to read each stream to a dataframe
                # Then add the dataframe to a list that we will concat together at the end
                dataframeLst = [self.__bq.storageClient.read_rows(stream.name,timeout = 9999).to_dataframe(session) for stream in session.streams]
                return pd.concat(dataframeLst)
            except:
                #if this fails using storage client default back to base BQ SQL
                print('BQ Storage Client comptability/version error. Using slower base BQ client')
                SQL_Select="""select * from
                `"""+project_name+'.'+dataset+'.'+table_name+'`;'
                return self.__bq.SQL.to_df(SQL_Select)

        def upload(self,Source, table_name, dataset=None, project_name=None, if_exists='replace',\
                       SourceType='auto',xlsxSheet=0):
            """Create or Replace a specified table in a specified dataset
            Source: Pandas DataFrame, OR the file name of type SourceType to get data from
            table_name: BQ table to create or replace in dataset.
                Note: If table_name is delimited by '.' will attempt to parse this to get dataset, table_name
                (and project_name if present)
            dataset: (None) BQ Dataset to write table to
            if_exists:('replace') One of 'fail', 'replace', or 'append'. BQ SQL behavior if Table name is in use.
            project_name: (None) BQ project to write to, default None uses Class level value 
            SourceType:(auto) 'auto' or one of 'df' for pandas dataframe, 'csv' for a csv file, or 'xlsx' for excel file.
                    'auto' checks for the full string extensions .xlsx or .csv in source, will default to df 
                    if these aren't present 
                    'csv' and 'xlsx' will infer headers and take whole csv/sheet. 'xlsx' can only do one sheet (at a time)
            xlsxSheet:(0) Only used when SourceType='xlsx', takes name of specific sheet to read data from,
                    default 0 will take the first (or only) sheet.
            """
            if SourceType=='csv' or (SourceType=='auto' and '.csv' in Source):
                df=pd.read_csv(Source)
            elif SourceType=='xlsx' or (SourceType=='auto' and '.xlsx' in Source):
                df=pd.read_excel(Source,sheet_name=xlsxSheet)
            else:
                df=Source

            #Pandas can have space or chars in column headers that break GBQ, use _ to replace these
            df.columns=df.columns.str.replace(r'[\W_]+','_')

            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name)

            DestTable=dataset+'.'+table_name
            df.to_gbq(DestTable, project_id=project_name, if_exists=if_exists, credentials=self.__bq.credentials) 


        def quick_view(self,table_name, dataset=None, project_name=None):
            """Quickly expore a new table, returns row count and top 100 rows in output
            If project_name/dataset for the table is None, assumes class default project_name
            Note: If table_name is delimited by '.' will attempt to parse this to get dataset, table_name
            (and project_name if present)
            """
            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name) 
            cnt=self.__bq.SQL.to_df("Select count(*) from `{0}.{1}.{2}`".format(project_name, dataset, table_name))
            print('Table has ',cnt.iloc[0,0],' rows')
            self.__PassRowsforDetExTable=cnt.iloc[0,0]
            return self.__bq.SQL.to_df("SELECT * from `{0}.{1}.{2}` LIMIT 100".format(project_name, dataset, table_name))

        def detail_view(self,table_name, dataset=None, project_name=None,Outputxlsx=None):
            """Writes a detailed sample of a new table to an excel file, inc top 100 rows
            and list of existing columns with Count of distinct value, min, and max. This can take several 
            minutes on a large table (1 million+ Rows). Note that very large tables with many columns
            and 10MM+ rows can time out by taking over an hour.
            If project_name for the table is None, assumes class default project_name
            If Outputxlsx is None, excel is named 'Review_of_'+table_name+'.xlsx' 
            (Names need to include the extension)
            Note: If dataset is delimited by '.' will attempt to parse this to get dataset, table_name
            (and project_name if present)"""

            project_name,dataset,table_name=self.__bq._BQ_User__nameDeparse(table_name,dataset,project_name)

            if Outputxlsx is None:
                Outputxlsx = 'Review_of_'+table_name+'.xlsx'

            top100=self.quick_view(table_name,dataset,project_name=project_name)

            if len(top100)<10:
                print('Fewer than 10 rows, just returning whole dataset')
                return top100

            SqlSummary="Select count(*) as Cnt_All_Rows"

            for x in top100.columns:
                SqlSummary=SqlSummary + ",count(distinct "+x+") AS CntDist_"+x
                SqlSummary=SqlSummary + ",Min("+x+") AS Min_"+x
                SqlSummary=SqlSummary + ",Max("+x+") AS Max_"+x

                #consider n/a nulls for strings, but avoid comparison to other dtypes so bqa doesn't barf
                if 'object' in str(top100[x].dtype):
                    SqlSummary=SqlSummary + ",SUM(Case when "+x+" is null or CAST("+x+\
                                    " AS STRING) in ('n/a','N/A','NA') then 1 else 0 end) AS Null_"+x
                else:
                    SqlSummary=SqlSummary + ",SUM(Case when "+x+" is null then 1 else 0 end) AS Null_"+x

            SqlSummary=SqlSummary+" from `{0}.{1}.{2}`".format(project_name, dataset, table_name)
            Summarydf=self.__bq.SQL.to_df(SqlSummary)

            OutdfList=[]
            NullHeader="Null, NA of "+str(self.__PassRowsforDetExTable)+" rows"
            for x in top100.columns:
                #Bc Excel will convert individual objects, datetimes for these mixed columns have to already
                #Strip any timezones to prevent excel from failing
                if 'datetime64' in str(top100[x].dtype):
                    top100[x]=top100[x].dt.tz_localize(None)
                    Summarydf['Min_'+x]=Summarydf['Min_'+x].dt.tz_localize(None)
                    Summarydf['Max_'+x]=Summarydf['Max_'+x].dt.tz_localize(None)

                PctNull=Summarydf['Null_'+x].item()/self.__PassRowsforDetExTable
                # Note: this will break if fewer than 3 rows, but we return the full dataset above so that shouldn't be an issue
                OutdfList.append(pd.DataFrame({'Field_Name': [x],'Definition,Notes':[''],\
                                        'Distinct_Values': [Summarydf['CntDist_'+x].item()],\
                                     NullHeader: [Summarydf['Null_'+x].item()],'Null %': [PctNull],\
                                    'Min': [Summarydf['Min_'+x].item()],'Max': [Summarydf['Max_'+x].item()],\
                                    'Sample_1':[top100.loc[0,x]],'Sample_2':[top100.loc[1,x]],\
                                     'Sample_3':[top100.loc[2,x]]}))

            Outdf=pd.concat(OutdfList)
            format_XLS(Outputxlsx,[[Outdf,'Field Summaries'],[top100,'Top 100']])

    
            
class GC_User():
    """Quick Cloud Upload and Download of files """  
    def __init__(self,bucket,project_name='analytics-pricing-thd'):
        from google.cloud import storage
        self.project_name= project_name
        self.bucket_name = bucket
        self.storage_client = storage.Client(self.project_name)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    
    def upload(self,file_name,DeleteLocal=True):
        """Upload file from local hard drive, deletes local copy if DeleteLocal=True"""
        blob = self.bucket.blob(file_name)
        blob.upload_from_filename(file_name)
        if DeleteLocal:
            os.remove(file_name)
            
    def download(self,file_name, LoadToRAMas=None):
        """Downloads file onto a file on the local hard drive
        LoadToRAMas = if not None then return the python object for the downloaded cloud values and delete Hard Drive copy
            Currently values can be: 'np' (Numpy saved Array/ np.savez_compressed)
            'pkl' (Generic Pickle Load Object)
            
            """
        blob = self.bucket.blob(file_name)
        with open(file_name,'wb') as fileObj:
            blob.download_to_file(fileObj)
            
        if LoadToRAMas:
            if LoadToRAMas=='np':
                return self.__load_to_ram_np_array(file_name)
            elif LoadToRAMas=='pkl':
                return self.__load_to_ram_pkl(file_name)            
            
            
            np.load('PredFeatures.npz',allow_pickle=True) # Load with allow pickle to get column names, possible future text IDs
    def __load_to_ram_np_array(self,file_name):
        """For ConvLSTM load saved numpy objects from cloud deleting the local copy"""
        try:
            FileValues=np.load(file_name)
        except:
            try: # Object or text in np compressed needs allow pickle to get column names, check this failure point
                FileValues=np.load('PredFeatures.npz',allow_pickle=True) # Load with allow pickle to get column names, possible future text IDs
            except:
                FileValues=None
                print('Numpy Load failed, are you sure this is a Numpy file?')
        os.remove(file_name)
        return FileValues
    
    def __load_to_ram_pkl(self,file_name):
        """For ConvLSTM load saved numpy objects from cloud deleting the local copy"""
        try:
            FileValues=pickle.load( open( file_name, "rb" ) )
        except:
            FileValues=None
            print('Pickle Load failed, are you sure this is a Python Pickle?')
        os.remove(file_name)
        return FileValue
    
    def __blob_string_name_matching(self,All_Blobs,StringsInNames):
        """Internal implementation of Blob Name String matching to be shared by both get Blob details 
                and Delete Blobs Method"""
        waslength=len(All_Blobs)
        if type(StringsInNames) not in (list, tuple): #Explicit list handling for for loop
            StringsInNames=[StringsInNames]
        for ContainsString in StringsInNames: #reduce list to only Blobs matching Criteria
            All_Blobs=[x for x in All_Blobs if ContainsString in x.name]
        print('String match removed:',waslength-len(All_Blobs),' Blobs ',len(All_Blobs),' remain in list')
        return All_Blobs
    
    def get_blobs_list(self,StringsInNames=None):
        """This returns the list of Blobs in the designated bucket as a dataframe with size and date created"""
        All_Blobs=list(self.bucket.list_blobs())
        if StringsInNames:
            All_Blobs=self.__blob_string_name_matching(All_Blobs,StringsInNames)
        BlobDFlist=[]
        for bb in All_Blobs:
            BlobDFlist.append(pd.DataFrame({'File_Name': [bb.name], 'Size': [HumanBytes.format(bb.size)],'Date Created':\
                                            [bb.time_created.strftime('%m-%d-%Y %I:%M:%S %p')]}))
        return pd.concat(BlobDFlist).reset_index(drop=True)
    
    def delete_blobs(self,Identifiers,Identifier_Match='Exact Names'):
        """Delete a Blob or list of Blobs designated by Identifiers (can be string or list)
            Identifier_Match='Exact Names' This checks the full blob name including .extension matches for deleting
            The other option is Identifier_Match='String Match in Names' in this case any Blob whose name CONTAINS
            all of the strings in the list (or just the one single string) is deleted
        """
        All_Blobs=list(self.bucket.list_blobs())
        if type(Identifiers)!=list: #Explicit list handling for for loop
                Identifiers=[Identifiers]
        if Identifier_Match=='Exact Names':
            for bb in All_Blobs:
                if bb.name in Identifiers:
                    bb.delete()    
        elif Identifier_Match=='String Match in Names':
            All_Blobs=self.__blob_string_name_matching(All_Blobs,Identifiers)
            for bb in All_Blobs:
                bb.delete()
        else:
            print("""Identifier_Match can only be either 'Exact Names' or 'String Match in Names'""")
