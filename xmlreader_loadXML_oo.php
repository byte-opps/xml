<?php
# 
# Usage php scriptname $1=XMLfilepath <$2=tablename>
#

include 'loadXMLclass.php'; 

# die ("Can't load class file loadXMLclass.php");

# connect to database
$PDO_connection_string='sqlite:xml.sqlite3';
$xmlloader= new loadXMLclass( $XMLfilepath=$argv[1], $InfoOnly=false, $PDO_connection_string );

#$xml_file_name
if(empty($argv[1])) { $xmlloader->AddToErrorsArray("Please specify xml file to parse!"); die('Please specify xml file to parse!'); }

# Find record node tag name
#$XML_node_name = $xmlloader->findRecordNodeName();

# if user suggest table name use that else default table.
isset($argv[2]) ? $tablename=$argv[2] : $tablename='LoadXML';
$xmlloader->CreateTables($tablename);

# load file into table
$xmlloader->LoadXML();

# show messages

if ( $msg = $xmlloader->GetMsgArray() 		)	{ echo "Messages:"; print_r($msg);  }			else echo "No Messages \n ";
if ( $errors = $xmlloader->GetErrorsArray() )  	{ echo "Errors:"; print_r($errors); }			else echo "No Errors   \n ";

# write log
$result = $xmlloader->GetResults();
echo "\n\r $result";

?>