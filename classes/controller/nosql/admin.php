<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		Example
 * @category		SimpleDB
 * @category		AmazonWebServices
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
class Controller_NoSQL_Admin extends Controller_Template_Twig
{
	/**
	 *
	 *
	 *
	 */
	public function before ()
	{
		parent::before();

		$configurations = Kohana::$config->load('nosql');

		$databases = array();

		foreach ($configurations AS $key => $config)
		{
			if ( ! isset($databases[$config['type']]) OR ! is_array($databases[$config['type']]))
			{
				$databases[$config['type']] = array();
			}

			$driver = NoSQL::instance($key);

			$tables = $driver->list_all();

			$databases[$config['type']][$key] = array(
				'config'	=>	$config,
				'tables'	=>	$tables
			);
		}

		$this->template->set('databases', $databases);
	}

	public function action_index()
	{
		
	}
	
	/**
	 *
	 *
	 *
	 */
	public function after ()
	{
		$this->template->set('base_url', BASEURL);

		if (Kohana::$profiling === TRUE) {
			$this->template->set('profiler', View::factory('profiler/stats'));
		}

		parent::after();
	}
}
