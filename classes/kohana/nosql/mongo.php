<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		MongoDB
 * @uses		http://www.php.net/manual/en/book.mongo.php
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
class Kohana_NoSQL_Mongo extends NoSQL
{
	// holds reference to MongoDB
	protected $_mongo;

	// Configuration array
	protected $_config;

	public function __construct($name, array $config)
	{
		parent::__construct($name, $config);

		$options = array();

		$timeout = (int) $this->_config['timeout'];
		if (array_key_exists('timeout', $this->_config) AND $timeout > 0) {
			$options['timeout'] = $timeout;
		} else {
			$options['timeout'] = 100;
		}

		if (array_key_exists('database', $this->_config) AND ! empty($this->_config['database'])) {
			$options['db'] = $this->_config['database'];
		}

		if (array_key_exists('replicaSet', $this->_config) AND ! empty($this->_config['replicaSet'])) {
			$options['replicaSet'] = $this->_config['replicaSet'];
		}

		if (array_key_exists('connect', $this->_config) AND ! empty($this->_config['connect'])) {
			$options['connect'] = $this->_config['connect'];
		}

		if (array_key_exists('username', $this->_config) AND ! empty($this->_config['username'])) {
			$options['username'] = $this->_config['username'];
		}

		if (array_key_exists('password', $this->_config) AND ! empty($this->_config['password'])) {
			$options['password'] = $this->_config['password'];
		}

		$this->_mongo = new Mongo($this->_config['server'], $options);

		$this->_mongodb = $this->_mongo->selectDB($this->_config['database']);
	}

	/**
	 * returns instance of Mongo
	 *
	 *
	 * @return 	Mongo
	 */
	public function mongo()
	{
		return $this->_mongo;
	}

	/** Data  Methods (Domains) **/

	/**
	 * This method is used to create capped collections and other collections requiring special options.
	 *
	 * @param	string    	collection name
	 * @param	array 		options array('capped'	=> (bool), 'size' => (int), 'max' => (int))
	 * @return	MongoCollection
	 */
	public function create_store($collection, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		// verified all options exist with defaults or user values
		$options['capped'] = (array_key_exists('capped', $options)) ? (bool) $options['capped'] : false ;
		$options['size'] = (array_key_exists('size', $options)) ? (int) $options['size'] : 0 ;
		$options['max'] = (array_key_exists('max', $options)) ? (int) $options['max'] : 0 ;

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$create_benchmark = Profiler::start(__FUNCTION__, 'MongoDB::createCollection');
		}

		if ($response = $this->_mongodb->createCollection($collection, $options['capped'], $options['size'], $options['max']))
		{
			if (isset($create_benchmark)) Profiler::stop($create_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoDB::createCollection: ';
				var_dump($response);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return ($response->w > 0) ? true : false ;
		}

		if (isset($create_benchmark)) Profiler::stop($create_benchmark);

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 *
	 *
	 * @throws	Kohana_Exception
	 */
	public function update_store($collection, Array $options=array())
	{
		throw new Kohana_Exception('Can not update Mongo collection');
	}

	/**
	 * Drops collection and deletes its indices
	 *
	 * @param	mixed		collection name or MongoCollection
	 * @return	bool
	 */
	public function delete_store($collection)
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		// make sure we have a MongoCollection object before we continue
		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$del_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::delete_store');
			}

			$response = $collection->drop();

			if (isset($del_benchmark)) Profiler::stop($del_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::delete_store: ';
				var_dump($response);
			}

			if ($this->_isOK($response))
			{
				return true;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);
		}

		return false;
	}

	/**
	 * Returns count of items items in mongo collection
	 *
	 * @uses	http://www.php.net/manual/en/mongocollection.count.php
	 *
	 * @param	string    	collection name
	 * @param	array 		query to match count
	 * @return	int
	 */
	public function count($collection, Array $query=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$find_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::count');
			}

			$count = $collection->count($query);

			if (isset($find_benchmark)) Profiler::stop($find_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::count: ';
				var_dump($count);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return (int) $count;
		}

		return false;
	}

	/**
	 * List all of the colllections in mongo database
	 *
	 * @return	array
	 */
	public function list_all(Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$list_benchmark = Profiler::start(__FUNCTION__, 'MongoDB::listCollections');
		}

		$list = $this->_mongodb->listCollections();

		if (isset($list_benchmark)) Profiler::stop($list_benchmark);

		if (Kohana::$environment >= $this->_debug)
		{
			echo 'MongoDB::listCollections: ';
			var_dump($list);
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return $list;
	}

	/**
	 * Returns information about the domain, including when the domain was created, the number of items
	 * and attributes in the domain, and the size of the attribute names and values.
	 *
	 * @param	string		domain name
	 * @return	array
	 */
	public function describe($domain)
	{
		throw new Kohana_Exception('Can not describe Mongo collection');
	}

	/** Data  item Methods (Items) **/

	/**
	 * selects and returns a document from a mongo collection
	 *
	 * @param	string		domain name
	 * @param	string		item name
	 * @param	array 		attributes to return
	 * @return	array
	 */
	public function get($collection, $item_name, Array $fields=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$find_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::get');
			}

			if(is_array($item_name))
				$item = $collection->findOne($item_name, $fields);
			else
			{
				try
				{
					$item = $collection->findOne(array('_id' => new MongoId($item_name)), $fields);
				}
				catch(Exception $e)
				{
					return false;
				}
			}

			if (isset($find_benchmark)) Profiler::stop($find_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::get: ';
				var_dump($item);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $item;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * selects and returns documents matching mongo query
	 *
	 * @uses	http://www.php.net/manual/en/mongocollection.find.php
	 *
	 * @param	string		collection name
	 * @param	array		mongo db query
	 * @param	array 		attributes to return
	 * @return	MongoCursor
	 */
	public function get_items($collection, Array $query=array(), Array $fields=array(), array $sort=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$find_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::get_items');
			}

			$start = $limit = FALSE;

			if ( array_key_exists('$limit', $query) AND array_key_exists('$offset', $query) )
			{
				$start = $query['$offset'];
				$limit = $query['$limit'];
			}
			unset($query['$limit']);
			unset($query['$offset']);

			$items = $collection->find($query, $fields);

			if (count($sort) > 0)
			{
				$items->sort($sort);
			}

			if ($start !== FALSE AND $limit !== FALSE)
			{	
				$items->skip($start)->limit($limit);
			}

			if (isset($find_benchmark)) Profiler::stop($find_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::get_items returned ' . (int) $items->count() . ' documents' . PHP_EOL;
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $items;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * Inserts a new Mongo db document into collection
	 *
	 * @uses	http://www.php.net/manual/en/mongocollection.insert.php
	 *
	 * @param	string		collection name
	 * @param	array 		options array('item' => array(), MORE OPTIONS)
	 * @return	bool
	 * @throws	Kohana_Exception		if options['item'] is not set
	 */
	public function put($collection, Array $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		if ( ! array_key_exists('item', $options) OR count($options['item']) <= 0) {
			throw new Kohana_Exception('Mongo put item_name is required');
		}
		$item = $options['item'];
		unset($options['item']);

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$ins_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::put');
			}

			$result = $collection->insert($item, $options);

			if (isset($ins_benchmark)) Profiler::stop($ins_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::put: ';
				var_dump($result);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $result;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * Updates or upserts a mongo db item
	 *
	 * @uses	http://www.php.net/manual/en/mongocollection.update.php
	 *
	 * @param	string		collection name
	 * @param	array 		options array('item_name' => $item_name, replace' => $replace, 'opt' => $opt)
	 * @return	bool
	 * @throws	Kohana_Exception		if item['item_name'] is not set
	 */
	public function update($collection, $query, Array $updates=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		// if no updates, then all is updated
		if (count($updates) <= 0) return true;

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$find_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::update');
			}

			$result = $collection->update($query, $updates);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::update: ';
				var_dump($result);
			}

			if (isset($find_benchmark)) Profiler::stop($find_benchmark);

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $result;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * removes matching documents from a mongo collection
	 *
	 * @uses	http://www.php.net/manual/en/mongocollection.remove.php
	 *
	 * @param	string		collection name
	 * @param	array		mongodb query
	 * @param	array		options
	 * @return 	bool
	 */
	public function delete($collection, $query, $options=array())
	{
		if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
			$benchmark = Profiler::start(__FUNCTION__, __METHOD__);
		}

		$collection = $this->_select($collection);

		if ($collection !== false)
		{
			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$find_benchmark = Profiler::start(__FUNCTION__, 'MongoCollection::delete');
			}

			$result = $collection->remove($query, $options);

			if (isset($find_benchmark)) Profiler::stop($find_benchmark);

			if (Kohana::$environment >= $this->_debug)
			{
				echo 'MongoCollection::delete: ';
				var_dump($result);
			}

			if (isset($benchmark)) Profiler::stop($benchmark);

			return $result;
		}

		if (isset($benchmark)) Profiler::stop($benchmark);

		return false;
	}

	/**
	 * same as ::get_items($collection, $query, array())
	 *
	 * @param	string		collection name
	 * @param	array		mongo db query
	 * @return	MongoCursor
	 */
	public function query($collection, $query=array())
	{
		return $this->get_items($collection, $query);
	}

	/**
	 *
	 *
	 * @param	mixed		MongoCollection or collection name
	 * @return 	MongoCollection
	 */
	protected function _select($collection)
	{
		if (is_string($collection))
		{
			if (Kohana::$environment >= $this->_debug)
			{
				echo 'Selecting collection: ' . $collection . PHP_EOL;
			}

			if ($this->_config['profiling'] === TRUE AND Kohana::$profiling === TRUE) {
				$sel_benchmark = Profiler::start(__FUNCTION__, 'MongoDB::selectCollection');
			}

			$collection = $this->_mongodb->selectCollection($collection);

			if (isset($sel_benchmark)) Profiler::stop($sel_benchmark);

			return $collection;
		}
		elseif ($collection instanceof MongoCollection)
		{
			return $collection;

		}

		return false;
	}

	protected function _isOK($response)
	{
		if ( ! is_array($response)) return false;

		if (array_key_exists('ok', $response) AND (int) $response['ok'] === 1)
		{
			if (Kohana::$environment >= $this->_debug)
				echo 'Response is OK' . PHP_EOL;
			return true;
		}

		return false;
	}

	public function disconnect()
	{
		unset($this->_mongo);

		parent::disconnect();
	}
}
