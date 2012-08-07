<?php defined('SYSPATH') or die('No direct access allowed.');
/**
 * NoSQL Auth driver
 *
 * [!!] this Auth driver does not support roles
 *
 * @package    		Kohana/Auth
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 * @abstract
 */
abstract class Kohana_Auth_NoSQL extends Auth
{
	/**
	 * Constructor loads the user list into the class.
	 */
	public function __construct($config = array())
	{
		parent::__construct($config);

		$this->db = NoSQL::instance($this->_config['database']);
	}

	/**
	 * Compare password with original (plain text). Works for current (logged in) user
	 *
	 * @param	string		$password
	 * @return 	boolean
	 */
	public function check_password($password)
	{
		$user = $this->get_user();

		if ( ! array_key_exists('username', $user) OR $user['username'] === FALSE)
		{
			return FALSE;
		}

		return ($password === $this->hash($user['password'], $user['username']));
	}
} // End Auth NoSQL
