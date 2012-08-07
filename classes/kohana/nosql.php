<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
abstract class Kohana_NoSQL
{
	/**
	 * @var  string  default instance name
	 */
	public static $default = 'default';

	/**
	 * @var  array  NoSQL instances
	 */
	public static $instances = array();

	/**
	 * Get a singleton NoSQL instance. If configuration is not specified,
	 * it will be loaded from the nosql configuration file using the same
	 * group as the name.
	 *
	 *     // Load the default nosql db
	 *     $data = NoSQL::instance();
	 *
	 *     // Create a custom configured instance
	 *     $data = NoSQL::instance('custom', $config);
	 *
	 * @param	string		instance name
	 * @param	array 		configuration parameters
	 * @return 	NoSQL
	 */
	public static function instance($name = NULL, array $config = NULL)
	{
		if ($name === NULL)
		{
			// Use the default instance name
			$name = NoSQL::$default;
		}

		if ( ! isset(NoSQL::$instances[$name]))
		{
			if ($config === NULL)
			{
				// Load the configuration for this database
				$config = Kohana::$config->load('nosql')->$name;
			}

			if ( ! isset($config['type']))
			{
				throw new Kohana_Exception('NoSQL Database type not defined in :name configuration',
					array(':name' => $name));
			}

			// Set the driver class name
			$driver = 'NoSQL_'.ucfirst($config['type']);

			// Create the nosql db connection instance
			new $driver($name, $config);
		}

		return NoSQL::$instances[$name];
	}

	// Configuration array
	protected $_config;

	/**
	 * Stores the nosql db configuration locally and name the instance.
	 *
	 * [!!] This method cannot be accessed directly, you must use [NoSQL::instance].
	 *
	 * @return  void
	 */
	protected function __construct($name, array $config)
	{
		// Set the instance name
		$this->_instance = $name;

		// Store the config locally
		$this->_config = $config;

		// Store the nosql instance
		NoSQL::$instances[$name] = $this;
	}

	/**
	 * Disconnect from the nosql db when the object is destroyed.
	 *
	 *     // Destroy the nosql db connection instance
	 *     unset(NoSQL::instances[(string) $nosql], $nosql);
	 *
	 * [!!] Calling `unset($nosql)` is not enough to destroy the nosql db connection, as it
	 * will still be stored in `NoSQL::$instances`.
	 *
	 * @return  void
	 */
	final public function __destruct()
	{
		$this->disconnect();
	}

	/**
	 * Returns the nosql instance name
	 *
	 *     echo (string) $nosql;
	 *
	 * @return  string
	 */
	final public function __toString()
	{
		return $this->_instance;
	}

	// default debug level
	protected $_debug = 50;

	/**
	 * Sets debug on or off
	 *
	 * 	Kohana::PRODUCTION  = 10;
	 *	Kohana::STAGING     = 20;
	 *	Kohana::TESTING     = 30;
	 *	Kohana::DEVELOPMENT = 40;
	 *
	 * @param	int		Kohana::DEVELOPMENT
	 * @return	bool
	 */
	final public function debug($environment=Kohana::DEVELOPMENT)
	{
		$this->_debug = $environment;
	}

	/**
	 * Disconnect from the database. This is called automatically by [NoSQL::__destruct].
	 * Clears the nosql db instance from [NoSQL::$instances].
	 *
	 * @return  boolean
	 */
	public function disconnect()
	{
		unset(NoSQL::$instances[$this->_instance]);

		return TRUE;
	}

	/** Data Store Methods (Tables, Documents, Collections, Domains) **/

	/**
	 * Creates a data store in current nosql db
	 *
	 * @param	string    	name
	 * @param	array 	    	table options - unique to each nosql db type
	 * @return	bool
	 */
	abstract public function create_store($name, Array $options=array());

	/**
	 * Update NoSQL data store
	 *
	 * @param	string		name
	 * @param	array 	    	options - unique to each nosql db type
	 * @return	array
	 */
	abstract public function update_store($name, Array $options=array());

	/**
	 * Deletes a data store
	 *
	 * @param	string		name
	 * @return	array
	 */
	abstract public function delete_store($name);

	/**
	 * Returns count of items/documents/records in data store
	 *
	 * @param	string    	name
	 * @return	int
	 */
	abstract public function count($name);

	/**
	 * List all of the data stores in the nosql database.
	 *
	 * @return	array
	 */
	abstract public function list_all(Array $options=array());

	/**
	 * Retrieves information about data store (table, domain, document)
	 *
	 * @param	string		name
	 * @return	array
	 */
	abstract public function describe($name);

	/** Data Store item Methods (Items) **/

	/**
	 * Retrieves a set of Attributes for an item that matches the primary key
	 *
	 * @param	string    	data store name
	 * @param	string    	primary_key for item to update
	 * @param	array 		options - unique to each nosql db type
	 * @return	array
	 */
	abstract public function get($name, $primary_key, Array $options=array());

	/**
	 * Creates a new item, or replaces an old item with a new item
	 *
	 * @param	string    	data store name
	 * @param	array 		options - unique to each nosql db type
	 * @return	bool
	 */
	abstract public function put($name, Array $options=array());

	/**
	 * Retrieves a set of Attributes for an item that matches the primary key
	 *
	 * @param	string    	data store name
	 * @param	string    	primary_key for item to update
	 * @param	array 		options - unique to each nosql db type
	 * @return	bool
	 */
	abstract public function update($name, $primary_key, Array $options=array());

	/**
	 * Retrieves a set of Attributes for an item that matches the primary key
	 *
	 * @param	string    	data store name
	 * @param	string    	primary_key for item to delete
	 * @param	array 		options - unique to each nosql db type
	 * @return	bool
	 */
	abstract public function delete($name, $primary_key, $options=array());

	/**
	 * Search data store for items
	 *
	 *
	  * @param	string    	data store name
	 * @param	mixed 		options - unique to each nosql db type
	 * @return 	array
	 */
	abstract public function query ($name, $options=null);
}
