<?php
class loadXMLclass {
	
	private $file_db;
	private $count;
	private $errorsArray;
	private $msgArray;
	private $XMLfilepath;
	private $infoOnly;
	private $tablename;
	private $meta_tablename;
	private $lastMetaUpdateId;
	private $seconds;
	private $record_node_name;
	
	function __construct( $XMLfilepath, bool $infoOnly, string $PDO_connection_string='sqlite:xml.sqlite3'){
		$this->start_TS = date('U'); # seconds since Unix Epoch this is the batch load datetime - unique load id
		
		$this->infoOnly=$infoOnly;
		#$xml_file_name
		if( empty($XMLfilepath) ) { 
			echo $this->errorsArray[]=__FUNCTION__ . "Error: Please specify xml file to parse!"; 
		}
		# store for later use
		$this->XMLfilepath = $XMLfilepath;
		#connect_DB
		try {
			// Create (connect to) SQLite database in file   
			$this->file_db = new PDO($PDO_connection_string);   
			// set the PDO error mode to exception   
			$this->file_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		}
		catch(PDOException $e){
			echo $this->errorsArray[]=__FUNCTION__ . " DB Error: " . $e->getMessage(); 
			exit;
		}
		echo "Starting $this->start_TS \n";
	}	
	function AddToErrorsArray(string $errorString){
		$this->errorsArray[]=$errorString;
	}
	function GetErrorsArray(){
		return $this->errorsArray;
	}
	function AddToMsgArray(string $msgString){
		$this->msgArray[]=$msgString;
	}
	function GetMsgArray(){
		return $this->msgArray;
	}
	function GetResults(){
		return "$this->count records inserted in $this->seconds seconds, last metadata insert into $this->meta_tablename id=".$this->lastMetaUpdateId;
	}
	
	private Function findRecordNodeName(){
		#ReadXML
		$reader = new XMLReader();
		$count=0;
		if (!$reader->open($this->XMLfilepath)){
			echo $this->errorsArray[]=__FUNCTION__ . " Error : Failed to open XML filepath = $this->XMLfilepath" ;
			exit;
		}
		$count=100000; # limit to stop execution time from being too long.
		$this->msgArray[]="Info: 1st $count reads !  ";
		$i = 0;
		while( $i < $count) {	
			$reader->read();
			$i++;
			if ($reader->nodeType == XMLReader::END_ELEMENT) {
				$end_elements[$reader->name]='';
				continue; //skips the rest of the code in this iteration
			}
			// process $node...
			if ($reader->nodeType == XMLReader::ELEMENT ) {
				# Count each unique tag name 
			   if ( ! isset ($element[$reader->depth][$reader->name] )){
					$element[$reader->depth][$reader->name] = 1;
			   }else{
				    $element[$reader->depth][$reader->name]++ ;
					# result is array of depth by name by count of occurrence.
					# ie [1]([ns:CDSNetChange-All-MessageTypes] => 65338)
			   }
			}
		}
		# sort array by keys which are the xml depth. 
		ksort($element);
		$max =0;
		$stat=[];
		foreach ($element as $Depth => $value){
			foreach ($value as $ELEMENT => $Count){
				# print all elements with count of occurrences 
				if($this->infoOnly) echo "Depth=$Depth  ELEMENT=$ELEMENT  Count=$Count \n"; # info debug
			
				# capture the max occurrence
				if ($Count > $max ) $max = $Count; 
				if ( ! isset ($stat[$Count])){
					# array in order, by occurrence, name
					$stat[][$Count]=$ELEMENT;
					
				}
				# 
				if ($Depth == 0) continue;
				$nestedTags[$count/$Depth]=$ELEMENT; 
			}
		}
		

		$this->msgArray[]="Info: Max element tag count is $max "; # information
		# Analyse the Xml to find the record tags 
		$highestKey=max(array_keys($nestedTags));
		$this->msgArray[]="Info: Record_node_name is = '{$nestedTags[$highestKey]}' "; #info
		$reader->close();
		return $this->record_node_name=$nestedTags[$highestKey];
	}

	function CreateTables($tablename){
		try {
		
			#$file_db->exec("DROP TABLE IF EXISTS earthquakes");   
			// clean table name  
			$tablename = str_replace ('ns:', '', $tablename);
			$tablename = str_replace ('-', '_', $tablename);
			$this->msgArray[]="Info: Table name = ".$tablename;
			$this->tablename=$tablename;
			#CONSTRAINT c_start_TS REFERENCES $this->meta_tablename (start_TS)
			$this->msgArray[]="Info: XML table DDL =". $createTable="  CREATE TABLE IF NOT EXISTS $this->tablename	 (id INTEGER PRIMARY KEY, start_TS DATE, json TEXT) ";
			$this->file_db->exec($createTable);
			$this->meta_tablename=$tablename."_meta";
			$this->msgArray[]="Info: XML meta table DDL =". $createMetaTable="  CREATE TABLE IF NOT EXISTS $this->meta_tablename	 (id INTEGER PRIMARY KEY, tablename varchar (35), start_TS DATE  CONSTRAINT c_start_TS REFERENCES $this->tablename (start_TS), seconds DATE, record_count int, xml_file_name varchar (35), xml_file_size bigint, XML_node_name varchar (35), MsgArray TEXT, ErrorArray TEXT) ";
			$this->file_db->exec($createMetaTable);
		
		}
		catch(PDOException $e){
			echo $this->errorsArray[]=__FUNCTION__ . " DB Error : " . $e->getMessage();
			exit;
		}
	}

	function LoadXML(int $limit=10000000000){
		# log starting
		$this->logMetaData();
		# find the node name for each record
		$this->findRecordNodeName();
		#ReadXML
		$reader = new XMLReader();
		$this->count=0;
		if (!$reader->open($this->XMLfilepath)){
			echo $this->errorsArray[]=__FUNCTION__ . " Error : Failed to open XML file, filepath = $this->XMLfilepath" ;
			exit;
		}
		# fast forwards to the nodes that we are interested in.
		while($reader->read() && $reader->name != $this->record_node_name)
		{
			;
		}
		// Prepare INSERT statement 
		$this->msgArray[]="Info: Insert sql =".$insert = "INSERT INTO $this->tablename (id, start_TS, json)   VALUES (null, :start_TS, :json)";  
		$stmt = $this->file_db->prepare($insert);
		// Bind parameters to statement variables
		$stmt->bindParam(':start_TS', $this->start_TS);   
		$stmt->bindParam(':json', $json); 
		#loop over each XML element in record node
		while  ( $reader->read() ){
			if ( $reader->name == $this->record_node_name ){
				$this->count++;
				#print_r ($reader->readOuterXml());
				# remove starting name space
				$xmlnons = str_replace ('<ns:', '<', $reader->readOuterXml());
				#remove trailing name space 
				$xmlnons = str_replace ('</ns:', '</', $xmlnons);
				$xmlobj = new SimpleXMLElement ( $xmlnons ); 
				$json = json_encode($xmlobj);
				# enter into table 
				try{
					// Execute prepared insert statement
					$stmt->execute(); 
				}
				catch(PDOException $e){
					echo $this->errorsArray[]=__FUNCTION__ . " DB ERROR: Table insert failed: " . $e->getMessage();
				}
				#echo "Reader name is=".$reader->name;
				#$reader->next();
				#echo "Reader name is=".$reader->name;
			}
			if ($this->count > $limit ) {echo "    Breaks on ...\n";  $this->msgArray[]="Info:  Breaks on at $this->count"; break;} # debug breaks
		}
		# log when finished
		$this->logMetaData();
	}
	private function logMetaData(){
		$dtNow = date('U'); # seconds since Unix Epoch this is the batch load datetime - unique load id
		$this->seconds = $dtNow-$this->start_TS;
		# enter Meta data into table 
		if (isset($this->lastMetaUpdateId)){
			#then we have made an insert so now we need to update.
			try{  
				#after load update meta data			
				$this->msgArray[]="Info: UpdateMeta sql =".$update = "UPDATE $this->meta_tablename SET record_count = :record_count, seconds = :seconds, XML_node_name = :XML_node_name, msgArray = :msgArray,errorArray = :errorArray WHERE id=$this->lastMetaUpdateId"; 
				$stmt = $this->file_db->prepare($update);
				// Execute statement
				$stmt->execute(array( 	':record_count'=>$this->count,':seconds'=>$this->seconds,':XML_node_name'=>$this->record_node_name,':msgArray'=>$this->msgArray ? json_encode($this->msgArray) : 0 ,':errorArray'=>$this->errorsArray ? json_encode($this->errorsArray) : 0   ) ); 
			} catch(PDOException $e){
				echo __FUNCTION__ . "  DB ERROR: updating $this->meta_tablename: " . $e->getMessage() . "\n MsgArray:";
				#print_r($this->msgArray) ; 
				#print_r($e) ; 
				exit;
			}
		}else{
			try{
				# create meta data entry for start of process
				$this->msgArray[]="Info: InsertMeta sql =".$insert = "INSERT INTO $this->meta_tablename (id, tablename, start_TS, seconds, record_count, xml_file_name, xml_file_size, XML_node_name, msgArray, errorArray) VALUES (null, :tablename, :start_TS, :seconds, :record_count, :xml_file_name, :xml_file_size, :XML_node_name,:msgArray, :errorArray)"; 
				$stmt = $this->file_db->prepare($insert);
				// Execute statement
				$stmt->execute(array(':tablename'=>$this->tablename, ':start_TS'=>$this->start_TS, ':seconds'=>$this->seconds, ':record_count'=>$this->count, ':xml_file_name'=>$this->XMLfilepath, ':xml_file_size'=>filesize($this->XMLfilepath), ':XML_node_name'=>$this->record_node_name, ':msgArray'=>$this->msgArray ? json_encode($this->msgArray) : 0 , ':errorArray'=>$this->errorsArray ? json_encode($this->errorsArray) : 0   )); 
				# update the last update id for this run
				$this->lastMetaUpdateId = $this->file_db->lastInsertId();
			} catch(PDOException $e){
				echo __FUNCTION__ . "  DB ERROR: Inserting to $this->meta_tablename: " . $e->getMessage() ; 
				exit;
			}
			
		}
		
		
	}
}

?>