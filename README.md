# datapull
<br>

A node.js application that pulls data from an IBM Cloudant or Apache CouchDB database.  The application requires a configuration file: config.json that contains the parameters to control the pulling of data and output.  This application is run from a terminal / command prompt or can be invoked from a batch script. 

Application features include:

- Pull data from an existing database
 
- Output data in a CSV formatted file

- Select the database fields that are to be pulled

Installation
================================================================================

<b>

		Node.js and NPM are required to install and execute this application
</b>

Download the source files and place in a directory.  Change to the install directory where the files were placed. Run the following NPM command to install the require Node modules:

	npm install

Once the above has successfully completed the application can be run.  The configuration file provided in this install does not conatin a valid database definition yet the program execute an indicated that zero records where pulled as shown by the following console message.

<b>
pull4001i - DB Record count: 0</b>
<br>
<br>

<b>Note:</b> If the program is started without defining the configuration file parameter it will default to looking in the application directory for a configuration file named 'config.json'.

Configuration file
================================================================================

The configuration file defines the required and optional processing parameters needed to load the data.  The file is formatted using JSON (JavaScript Object Notation).  Below is an example of a configuration file defined with the <b>minimum</b> set of parameters:

	{		
  	  "database"  : { "dbname":"doctors", "url":"http://localhost:5984" },
      "inputfile" : { "filename":"/Users/daveweilert/GitHub/dataloader/example/000003_Data_File_TAB.txt"} 
    }

<b>Note</b> the use of double quotes, curly brackets, colons, commas, etc. in the definition of the parameter values. These are required to create a properly formatted JSON file.  Additional information regarding valid JSON can be obtained at: <http://www.json.org>

The above configuration file is using the "inputfile" parameter defaults a.) tab delimited fields b.) first record contains the field names, and c.) all input fields will be loaded. 

<br>

## Parameters

The key parameter and sub-parameters are case sensitive and only use lower case characters to define the values.  The users values that are provided for each parameter are not required to be lower case.  The following tables lists the parameter, required or optional, valid values, default value, description, sub-parameters, and example(s).  
<br>

### "batchsize"

| Req/Opt | Valid Value(s) | Default | Description |
| :----:  | -----          | :----:  | -----       |
| Opt | whole number greater than zero | 5000 | Defines the number of records that will be batched and passed to the database to be inserted.

Sub-parameters: NONE

Example: 
<br>
1 - Create batch of 5000 records
<b>
<br>

      "batchsize": 5000
</b>

<br>

