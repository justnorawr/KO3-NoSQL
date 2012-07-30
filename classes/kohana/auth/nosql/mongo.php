<?php defined('SYSPATH') or die('No direct access allowed.');
/**
 * NoSQL Auth driver
 *
 * [!!] this Auth driver does not support roles
 *
 * @package    		Kohana/Auth
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
class Kohana_Auth_NoSQL_Mongo extends Auth_NoSQL
{
	/**
	 * Logs a user in.
	 *
	 * @param 	string	 	username
	 * @param 	string   	password
	 * @param 	boolean	enable autologin (not supported)
	 * @return	boolean
	 */
	protected function _login($username, $password, $remember=false)
	{
		if (is_string($password))
		{
			// Create a hashed password
			$password = $this->hash($password, $username);
		}

		try
		{
			$query = array('username' => $username, 'password' => $password);

			// dont return password, so it does not get incuded in session
			$attrs = array('password' => -1);

			// query for user 
			$user = $this->db->get($this->_config['table_name'], $query, $attrs);

			if ($user->count() === 1)
			{
				return $this->complete_login($user[0]);
			}
		}
		catch (Exception $e)
		{
			return FALSE;
		}

		// Login failed
		return FALSE;
	}

	/**
	 * Forces a user to be logged in, without specifying a password.
	 *
	 * @param 	mixed		username
	 * @return	boolean
	 */
	public function force_login($username)
	{
		try
		{
			$query = array('username' => $username);

			// dont return password, so it does not get incuded in session
			$attrs = array('password' => -1);

			// query for user 
			$user = $this->db->get($this->_config['table_name'], $query, $attrs);

			if ($user->count() === 1)
			{
				return $this->complete_login($user[0]);
			}
		}
		catch (Exception $e)
		{
			return FALSE;
		}

		// Login failed
		return FALSE;
	}

	/**
	 * Get the stored password for a username.
	 *
	 * @param 	string		username
	 * @return	string
	 */
	public function password($username)
	{
		try
		{
			$query = array('username' => $username);

			// only return password
			$attrs = array('password');

			// query for user s password
			$user = $this->db->get($this->_config['table_name'], $query, $attrs);

			if ($user->count() === 1)
			{
				$password = $user[0]['password'];
				return $password;
			}
		}
		catch (Exception $e)
		{
			return FALSE;
		}

		// Login failed
		return FALSE;
	}

	/**
	 * Complete the login for a user by incrementing the logins and setting
	 * session data: user_id, username
	 *
	 * @param	string		username
	 * @return 	void
	 */
	protected function complete_login($user)
	{
		try
		{
			$query = array(
				'username'	=>	$user['username']
			);

			$updates = array(
				'logins'		=>	'$inc',
				'lastlogin'	=>	time()
			);

			$result = $this->db->update($this->_config['table_name'], $query, $updates);

			if ($result === TRUE) {
				return parent::complete_login($user);
			}
			else {
				return FALSE;
			}
		}
		catch (Exception $e)
		{
			return FALSE;
		}
	}
} // End Auth NoSQL Mongo
