<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		SimpleDB
 * @category		AmazonWebServices
 * @uses		http://docs.amazonwebservices.com/AWSSDKforPHP/latest/#i=AmazonSDB
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
class Kohana_NoSQL_AWS_Simpledb extends NoSQL_AWS
{
	// holds reference to AmazonSimpleDB
	protected $_simpledb;

	// Configuration array
	protected $_config;

	public function __construct ($name, array $config)
	{
		parent::__construct($name, $config);

		$aws_config = Kohana::$config->load('aws.credentials');
		$aws_config = (array) $aws_config;

		$this->_simpledb = new AmazonSDB($aws_config['@default']);
	}

	/**
	 * returns instance of AmazonSimpleDB
	 *
	 *
	 * @return 	AmazonSimpleDB
	 */
	public function simpledb ()
	{
		return $this->_simpledb;
	}

	/** Data Store Methods (Domains) **/

	/**
	 * The CreateDomain operation creates a new domain. The domain name should be unique among the
	 * domains associated with the Access Key ID provided in the request. The CreateDomain operation may
	 * take 10 or more seconds to complete.
	 *
	 * CreateDomain is an idempotent operation; running it multiple times using the same domain name will
	 * not result in an error response.
	 *
	 * @param	string    	domain name
	 * @return	bool
	 */
	public function create_store ($domain, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$create_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::create_domain');
		}

		$response = $this->_simpledb->create_domain($domain);

		if (isset($create_benchmark)) Profiler::stop($create_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->iSOK())
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
	 * updates simpledb domain table
	 *
	 * @throws	Kohana_Exception
	 */
	public function update_store ($domain, Array $options=array())
	{
		throw new Kohana_Exception('Can not update SimpleDB domain');
	}

	/**
	 * Deletes a domain and all of its items
	 *
	 * The DeleteDomain operation deletes a domain. Any items (and their attributes) in the domain are
	 * deleted as well. The DeleteDomain operation might take 10 or more seconds to complete.
	 *
	 *Running DeleteDomain on a domain that does not exist or running the function multiple times using
	 * the same domain name will not result in an error response.
	 *
	 * @param	string		domain name
	 * @return	bool
	 */
	public function delete_store ($domain)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$del_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::delete_domain');
		}

		$response = $this->_simpledb->delete_domain($domain);

		if (isset($del_benchmark)) Profiler::stop($del_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->iSOK())
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
	 * Returns count of items items in simpledb domain
	 *
	 * @param	string    	domain name
	 * @return	int
	 */
	public function count($domain)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$count_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::domain_metadata');
		}

		$response = $this->_simpledb->domain_metadata($domain);

		if (isset($count_benchmark)) Profiler::stop($count_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			// get count of items from meta data and return it
			$body = $response->body->to_array()->getArrayCopy();
			$count = (array) $body['DomainMetadataResult'];

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $count['ItemCount'];
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * List all of the domains in simpledb
	 *
	 ** @uses	http://docs.amazonwebservices.com/AWSSDKforPHP/latest/index.html#m=AmazonSDB/domain_metadata
	 *
	 * @param	array 		array('pcre' => REGEX) or array('max' => INT)
	 * @return	array
	 */
	public function list_all(Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if (array_key_exists('pcre', $options))
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$list_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::get_domain_list');
			}

			$response = $this->_simpledb->get_domain_list($options['pcre']);

			if (isset($list_benchmark)) Profiler::stop($list_benchmark);
		}
		elseif (array_key_exists('max', $options))
		{
			if ($options['max'] > 0)
			{
				if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
					$list_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::list_domains');
				}

				$response = $this->_simpledb->list_domains(array(
					'MaxNumberOfDomains' => $options['max']
				));

				if (isset($list_benchmark)) Profiler::stop($list_benchmark);
			}
		}

		if ( ! isset($response))
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$list_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::list_domains');
			}

			$response = $this->_simpledb->list_domains();

			if (isset($list_benchmark)) Profiler::stop($list_benchmark);
		}


		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$body = $response->body->to_array()->getArrayCopy();

			$domains = (array) $body['ListDomainsResult'];

			if (array_key_exists('DomainName', $domains) AND count($domains['DomainName']) > 0)
			{
				$domains = $domains['DomainName'];

				if ( ! is_array($domains)) {
					$domains = array(0 => $domains);
				}

				return $domains;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return array();
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * Returns information about the domain, including when the domain was created, the number of items
	 * and attributes in the domain, and the size of the attribute names and values.
	 *
	 * @uses	http://docs.amazonwebservices.com/AWSSDKforPHP/latest/index.html#m=AmazonSDB/domain_metadata
	 *
	 * @param	string		domain name
	 * @return	array
	 */
	public function describe ($domain)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$desc_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::domain_metadata');
		}

		$response = $this->_simpledb->domain_metadata($domain);

		if (isset($desc_benchmark)) Profiler::stop($desc_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$body = $response->body->to_array()->getArrayCopy();
			$meta_data = (array) $body['DomainMetadataResult'];

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $meta_data;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/** Data Store item Methods (Items) **/

	/**
	 * selections an item by item name
	 *
	 * @uses	http://docs.amazonwebservices.com/AWSSDKforPHP/latest/index.html#m=AmazonSDB/put_attributes
	 *
	 * @param	string		domain name
	 * @param	string		item name
	 * @param	array 		attributes to return
	 * @return	array
	 */
	public function get($domain, $item_name, Array $attributes=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		// if only one option flatten array
		if (count($attributes) == 1) {
			$attributes = $attributes[0];
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$get_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::get_attributes');
		}

		$response = $this->_simpledb->get_attributes($domain, $item_name, $attributes);

		if (isset($get_benchmark)) Profiler::stop($get_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

		if ($response->isOK())
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Response Is OK' . PHP_EOL;
			}

			$body = $response->body->to_array()->getArrayCopy();
			$attributes = (array) $body['GetAttributesResult']['Attribute'];

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $this->_process_attributes($attributes);
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * Creates or replaces attributes in an item
	 *
	 * @uses	http://docs.amazonwebservices.com/AWSSDKforPHP/latest/index.html#m=AmazonSDB/put_attributes
	 *
	 * @param	string		domain name
	 * @param	array 		options array('item_name' => $item_name, replace' => $replace, 'opt' => $opt)
	 * @return	bool
	 * @throws	Kohana_Exception		if item['item_name'] is not set
	 */
	public function put($domain, Array $item=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ( ! array_key_exists('item_name', $item) OR empty($item['item_name'])) {
			throw new Kohana_Exception('SimpleDB put item_name is required');
		}

		$item_name = $item['item_name'];
		unset($item['item_name']);

		$replace = true;
		if (array_key_exists('replace', $item)) {
			$replace = $item['replace'];
			unset($item['replace']);
		}

		$opt = null;
		if (array_key_exists('opt', $item)) {
			$opt = $item['opt'];
			unset($item['opt']);
		}

		$keypairs = $item;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$put_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::put_attributes');
		}

		$response = $this->_simpledb->put_attributes($domain, $item_name, $keypairs, $replace, $opt);

		if (isset($put_benchmark)) Profiler::stop($put_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

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

	public function update($domain, $item_name, Array $options=array())
	{
		return $this->put($domain, $item_name, $options+array('replace', true));
	}

	/**
	 * Deletes one or more attributes associated with the item. If all attributes of an item are deleted, the
	 * item is deleted.
	 *
	 * If you specify DeleteAttributes without attributes or values, all the attributes for the item are deleted.
	 *
	 * DeleteAttributes is an idempotent operation; running it multiple times on the same item or attribute
	 * does not result in an error response
	 *
	 * @param	string		domain name
	 *
	 * @return 	bool
	 */
	public function delete($domain, $item_name, $attributes=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$opt = null;
		if (array_key_exists('opt', $attributes)) {
			$opt = $attributes['opt'];
			unset($attributes['opt']);
		}

		if (count($attributes) <= 0) $attributes = null;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$del_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::delete_attributes');
		}

		$response = $this->_simpledb->delete_attributes($domain, $item_name, $attributes, $opt);

		if (isset($del_benchmark)) Profiler::stop($del_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'SimpleDBDriver Response: ';
			var_dump($response);
		}

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
	 * The Select operation returns a set of attributes for ItemNames that match the select expression.
	 * Select is similar to the standard SQL SELECT statement
	 *
	 * @param	string		domain name
	 * @param	array 		SimpleDB SQL statement
	 * @return 	array
	 */
	public function query ($domain, $statement=null)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if (is_null($statement)) {
			$statement = 'SELECT * FROM `' . $domain . '`';
		}

		$next_token = null;

		$returns = array();

		do
		{
			if ($next_token)
			{
				if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
					$sel_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::select(NextToken='.$next_token);
				}

				$response = $this->_simpledb->select($statement, array(
					'NextToken'	=>	$next_token
				));

				if (isset($sel_benchmark)) Profiler::stop($sel_benchmark);
			}
			else
			{
				if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
					$sel_benchmark = Profiler::start(__FUNCTION__, 'AmazonSDB::select');
				}

				$response = $this->_simpledb->select($statement);

				if (isset($sel_benchmark)) Profiler::stop($sel_benchmark);
			}

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'SimpleDBDriver Response: ';
				var_dump($response);
			}

			if ($response->isOK())
			{
				if (Kohana::$environment >= $this->_debug)
				{
					echo 'Response Is OK' . PHP_EOL;
				}

				$body = $response->body->to_array()->getArrayCopy();
				$items = $body['SelectResult']['Item'];

				foreach ($items AS $key =>$item)
				{
					$returns[$item['Name']] = $this->_process_attributes($item['Attribute']);
				}
			}

			$next_token = isset($response->body->SelectResult->NextToken)
				? (string) $response->body->SelectResult->NextToken
				: null;
		}
		while ($next_token);

		if (isset($benchmark)) Profiler::stop($benchmark);

		return $returns;
	}

	protected function _process_attributes ($object)
	{
		$columns = array();

		foreach ($object AS $attr)
		{
			$columns[$attr['Name']] = $attr['Value'];
		}

		return $columns;
	}

	public function disconnect ()
	{
		unset($this->_simpledb);

		parent::disconnect();
	}
}
