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

		$this->config = Model::factory('config');

		$this->AuthUser = Auth::instance()->get_user(false);
	}
	
	/**
	 *
	 *
	 *
	 */
	public function after ()
	{
		$this->template->set('version', $this->config->version);
		$this->template->set('base_url', BASEURL);

		$this->template->set('AuthUser', $this->AuthUser);

		if (Kohana::$profiling === TRUE) {
			$this->template->set('profiler', View::factory('profiler/stats'));
		}

		parent::after();
	}
}
