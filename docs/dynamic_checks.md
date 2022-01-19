## Current plan to generate data quality checks dynamically

1. Entries of a DynamoDB table
    * `col_nm` : `String`
    * `col_dsc` : `String`
    * `col_length` :`Number` - 0 if length = 0, numeric otherwise
    * `data_type`: `String Set` - Containing the following values
        * Boolean 
        * Fractional
        * Integral
        * Null
        * Numeric
        * String
    * `pk_ind` : `Boolean` - True if a column is PK and False otherwise
    * `null_ind` : `Boolean` - True if a column is nullable and False otherwise
    * `data_classification` - `string`
    * `req_tokenization` : `Boolean` True if Yes False otherwise
    

2. Extract the DynamoDB table using boto3 and create a list of items:
```
{'data_classification': {'S': 'non-confidential'},
 'source_col_name': {'S': 'patents'},
 'check_primary_key': {'BOOL': False},
 'col_description': {'S': 'No of patents filed by different universities'},
 'req_tokenization': {'BOOL': False},
 'check_is_null': {'BOOL': False},
 'col_name': {'S': 'patents'},
 'check_data_type': {'S': 'integer'},
 'col_id': {'N': '3'}}
 ```
 
3. For each column find out the different checks to be performed.
   * Currently we are performing 4 checks on the data:
       * Null Value check 
       * Primary Key check
       * Max Length Check
       * Data Type Check
   * Each check will be a function that will parse the item from the dynamoDB response
   * Each function will output a list of checks in string for e.g. ['.hasMax(column_name, lambda_function)']
   * A JSON file will map the key word of the check with the corresponding Pydeequ function
   * Create a string object from all the functions combined
   
4. Generating a string of text resembling the Pydeequ Function
5. Using `exec` method to run the pydeequ check string created dynamically