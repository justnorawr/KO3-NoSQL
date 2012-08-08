<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		DynamoDB
 * @category		AmazonWebServices
 * @uses		http://docs.amazonwebservices.com/AWSSDKforPHP/latest/#i=AmazonDynamoDB
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
class Kohana_NoSQL_AWS_Dynamo extends NoSQL_AWS
{
	// holds reference to AmazonDynamoDB
	protected $_dynamodb;

	// default time in seconds between select when polling
	protected $_create_interval = 1;
	protected $_delete_interval = 1;
	protected $_update_interval = 5;

	// default limit to sleep when polling
	protected $_default_sleep_limit = 100;

	// Configuration array
	protected $_config;

	public function __construct($name, array $config)
	{
		parent::__construct($name, $config);

		$aws_config = Kohana::$config->load('aws.credentials');
		$aws_config = (array) $aws_config;

		$this->_dynamodb = new AmazonDynamoDB( $aws_config['@default'] );
	}

	/**
	 * returns instance of AmazonDynamoDB
	 *
	 * @return 	AmazonDynamoDB
	 */
	public function dynamodb()
	{
		return $this->_dynamodb;
	}

	/** Data Store Methods (Tables) **/

	/**
	 * Creates a table in dynamodb
	 *
	 * @param	string    	table name
	 * @param	array 	    	table options - unique to each nosql db type
	 * @return	bool
	 */
	public function create_store($table, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$sleep_limit = $this->_default_sleep_limit;

		$options['TableName'] = $table;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$create_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::create_table');
		}

		$response = $this->_dynamodb->create_table($options);

		if (isset($create_benchmark)) Profiler::stop($create_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$count = 0;

			do
			{
				sleep($this->_create_interval);
				$count = $count + $this->_create_interval;

				$response = $this->_describe_table($table);
			}
			while ($count <= $sleep_limit AND (string) $response->body->Table->TableStatus !== 'ACTIVE');

			if (isset($benchmark)) Profiler::stop($benchmark);

			// loop was broken because of sleep_limit return false otherwise all is good
			return ($count <= $sleep_limit) ? true : false ;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		// table was not created
		return false;
	}

	/**
	 * updates dynamodb table
	 *
	 * @param	string		table name
	 * @param	array 	    	options
	 * @return	array
	 */
	public function update_store($table, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$sleep_limit = $this->_default_sleep_limit;

		$options['TableName'] = $table;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$update_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::update_table');
		}

		$response = $this->_dynamodb->update_table($options);

		if (isset($update_benchmark)) Profiler::stop($update_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$count = 0;

			do
			{
				sleep($this->_update_interval);
				$count = $count + $this->_update_interval;

				$response = $this->_describe_table($table);
			}
			while ($count <= $sleep_limit AND (string) $response->body->Table->TableStatus !== 'ACTIVE');

			if (isset($benchmark)) Profiler::stop($benchmark);

			// loop was broken because of sleep_limit return false otherwise all is good
			return ($count <= $sleep_limit) ? true : false ;
		}
		else
		{
			if (isset($benchmark)) Profiler::stop($benchmark);

			// table was not updated
			return false;
		}
	}

	/**
	 * Deletes a table and all of its items
	 *
	 * @param	string		table name
	 * @return	array
	 */
	public function delete_store($table)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$sleep_limit = $this->_default_sleep_limit;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$delete_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::delete_table');
		}

		$response = $this->_dynamodb->delete_table(array(
			'TableName' => $table
		));

		if (isset($delete_benchmark)) Profiler::stop($delete_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$count = 0;

			do
			{
				sleep($this->_delete_interval);
				$count = $count + $this->_delete_interval;

				$response = $this->_describe_table($table);
			}
			while ($count <= $sleep_limit AND (integer) $response->status !== 400);

			if (isset($benchmark)) Profiler::stop($benchmark);

			// loop was broken because of sleep_limit return false otherwise all is good
			return ($count <= $sleep_limit) ? true : false ;
		}
		else
		{
			if (isset($benchmark)) Profiler::stop($benchmark);

			// table was not deleted
			return false;
		}
	}

	/**
	 * Queries Amazon DynamoDB for ItemCount of table
	 *
	 * @warning	This method is not reliable for update to date count of records
	 * since it is updated approx every 6 hours via Amazon
	 *
	 * @param	string		table name
	 * @return	bool
	 */
	public function count($table)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$response = $this->_describe_table($table);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		 $item_count = (integer) $response->body->Table->ItemCount;

		 return $item_count;
	}

	/**
	 * List all of the tables in dynamodb
	 *
	 * @return	array
	 */
	public function list_all(Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$list_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::list_tables');
		}

		$response = $this->_dynamodb->list_tables();

		if (isset($list_benchmark)) Profiler::stop($list_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		$body = $response->body->to_array()->getArrayCopy();

		$tables = (is_array($body['TableNames'])) ? $body['TableNames'] : array(0 => $body['TableNames']) ;

		if (isset($benchmark)) Profiler::stop($benchmark);

		return $tables;
	}

	/**
	 * Retrieves information about the table, including the current status
	 * of the table, the primary key schema and when the table was created.
	 *
	 * @param	string		table name
	 * @return	array
	 */
	public function describe($table)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$response = $this->_describe_table($table);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		if ($response->isOK())
		{
			$body = $response->body->to_array()->getArrayCopy();

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $body['Table'];
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/** Data Store item Methods (Items) **/

	public function get($table, $primary_key, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$primary_key = (is_array($primary_key))
			? $this->_primary_key($primary_key[0], $primary_key[1])
			: $this->_primary_key($primary_key);

		$options['TableName'] = $table;
		$options['Key'] = $primary_key;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$get_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::get');
		}

		$response = $this->_dynamodb->get_item($options);

		if (isset($get_benchmark)) Profiler::stop($get_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$item = $response->body->Item->to_array()->getArrayCopy();
			$return = array();

			foreach ($item AS $key => $value)
			{
				$return[$key] = $this->_get_value($value);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $return;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * Retrieves the attributes for multiple items from multiple tables using their primary keys.
	 *
	 * The maximum number of item attributes that can be retrieved for a single operation is 100.
	 *	Also, the number of items retrieved is constrained by a 1 MB the size limit. If the response
	 *	size limit is exceeded or a partial result is returned due to an internal processing failure,
	 *	Amazon DynamoDB returns an UnprocessedKeys value so you can retry the operation starting
	 *	with the next item to get.
	 *
	 * Amazon DynamoDB automatically adjusts the number of items returned per page to enforce
	 *	this limit. For example, even if you ask to retrieve 100 items, but each individual item is
	 *	50k in size, the system returns 20 items and an appropriate UnprocessedKeys value so you
	 *	can get the next page of results. If necessary, your application needs its own logic to assemble
	 *	the pages of results into one set.
	 *
	 * @todo	implement UnprocessedKeys
	 *
	 * @param	string		table name
	 * @param	array 		array of primary keys array($hash, array($hash), array($hash, $range))
	 * @return 	array
	 */
	public function get_items($table, $keys)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$item_keys=  array();

		foreach ($keys AS $key)
		{
			$primary_key = (is_array($key))
				? $this->_primary_key($key[0], $key[1])
				: $this->_primary_key($key);

			$item_keys[] = $primary_key;
		}

		$options = array(
			'RequestItems' => array(
			$table => array(
				'Keys' => $item_keys,
			))
		);

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$get_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::batch_get');
		}

		$response = $this->_dynamodb->batch_get_item($options);

		if (isset($get_benchmark)) Profiler::stop($get_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$items = array();

			foreach ($response->body->Responses->{$table}->Items as $item)
			{
				$columns = array();

				foreach ($item AS $key => $value)
				{
					$columns[$key] = $this->_get_value($value);
				}

				$items[] = $columns;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $items;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	public function put($table, Array $item=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$options = array();
		$options['TableName'] = $table;
		$options['Item'] = $this->_dynamodb->attributes($item);

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$put_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::put');
		}

		$response = $this->_dynamodb->put_item($options);

		if (isset($put_benchmark)) Profiler::stop($put_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return true;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	public function update($table, $primary_key, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$primary_key = (is_array($primary_key))
			? $this->_primary_key($primary_key[0], $primary_key[1])
			: $this->_primary_key($primary_key);

		$options = array(
				'TableName'	=>	$table,
				'Key'		=>	$primary_key
		) + $options;

		// Updating an item
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$update_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::update');
		}

		$response = $this->_dynamodb->update_item($options);

		if (isset($update_benchmark)) Profiler::stop($update_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return true;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	public function delete($table, $primary_key, $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$primary_key = (is_array($primary_key))
			? $this->_primary_key($primary_key[0], $primary_key[1])
			: $this->_primary_key($primary_key);

		$options = array();
		$options['TableName'] = $table;
		$options['Key'] = $primary_key;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$del_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::delete');
		}

		$response = $dynamodb->delete_item($$options);

		if (isset($del_benchmark)) Profiler::stop($del_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return true;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 *
	 * @todo implement Count, Limit
	 *
	 * @param	string		table name
	 * @param	array 		query options
	 * @return 	array
	 */
	public function query ($table, $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$options['TableName'] = $table;

		$options['HashKeyValue'] = $this->_dynamodb->attributes(array($options['HashKeyValue']));
		$options['HashKeyValue'] = $options['HashKeyValue'][0];

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$query_benchmark = Profiler::start(__FUNCTION__, 'AmazonDynamoDB::query');
		}

		$response = $this->_dynamodb->query($options);

		if (isset($query_benchmark)) Profiler::stop($query_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		// Check for success...
		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$items = array();

			$responses = $response->body->Items->to_array()->getArrayCopy();

			if ($response->body->Count <= 1) $responses = array($responses);

			foreach ($responses as $item)
			{
				$columns = array();

				foreach ($item AS $key => $value)
				{
					$columns[$key] = $this->_get_value($value);
				}

				$items[] = $columns;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $items;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * type casts array value into flattened value
	 *
	 * @param	array 		array with type array('S') or array('N')
	 * @return 	string		type casted value
	 */
	protected function _get_value ($value)
	{
		$return = null;

		if (is_array($value))
		{

			if (array_key_exists('S', $value))
			{
				$return = (string) $value['S'];
			}
			elseif (array_key_exists('N', $value))
			{
				$return = (int) $value['N'];
			}
		}
		elseif (is_object($value))
		{
			if (property_exists($value, 'S'))
			{
				$value = (array) $value->S;
				$return = (string) $value[0];
			}
			elseif (property_exists($value, 'N'))
			{
				$value = (array) $value->N;
				$return = (string) $value[0];
			}
		}

		return $return;
	}

	/**
	 * Returns primary key for hash or hash-range keys
	 *
	 * @param	string		hash key
	 * @param	string		range key
	 * @return 	string
	 */
	protected function _primary_key ($HashKeyElement, $RangeKeyElement=NULL)
	{
		if ($RangeKeyElement !== NULL)
		{
			return $this->_dynamodb->attributes(array(
				'HashKeyElement'	=>	$HashKeyElement,
				'RangeKeyElement'	=>	$RangeKeyElement
			));
		}
		else
		{
			return $this->_dynamodb->attributes(array(
				'HashKeyElement'	=>	$HashKeyElement
			));
		}
	}

	protected function _describe_table ($table)
	{
		$response = $this->_dynamodb->describe_table(array(
			'TableName' => $table
		));

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'DynamoDriver Response: ';
			var_dump($response);
		}

		return $response;
	}

	public function disconnect ()
	{
		unset($this->_dynamodb);

		parent::disconnect();
	}
}
