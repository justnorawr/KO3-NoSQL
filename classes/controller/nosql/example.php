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
class Controller_NoSQL_Example extends Controller_Template_Twig
{
	public function before ()
	{
		parent::before();

		ob_start();
	}

	public function action_error()
	{

	}

	public function after ()
	{
		$output = ob_get_contents();

		ob_end_clean();

		$this->template->set('content', $output);

		parent::after();
	}
}
