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
