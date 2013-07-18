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
			$query = array('username' => $username, 'password' => $password, 'deleted' => 0);

			// query for user
			$results = $this->db->get_items($this->_config['table_name'], $query);

			if ($results->count() === 1)
			{
				$user = $results->getNext();
				
				unset($user['password']);

				return $this->complete_login($user, $remember);
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

			// query for user 
			$results = $this->db->get_items($this->_config['table_name'], $query);

			if ($results->count() === 1)
			{
				$user = $results->getNext();
				
				unset($user['password']);

				return $this->complete_login($user, FALSE);
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

			if (count($user) === 1)
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
	 * Gets the currently logged in user from the session.
	 * Returns NULL if no user is currently logged in.
	 *
	 * @return  mixed
	 */
	public function get_user($default = NULL)
	{
		$token = Cookie::get('ycmdautotoken');

		if ( ! empty($token) )
		{
			$query = array('token' => $this->_hashToken($token));
			$result = $this->db->get('user_tokens', $query, array('username'));

			if ($result) {
				Cookie::set('ycmdautotoken', $token, 86400*30);
				$this->force_login($result['username']);
			}
		}

		return $this->_session->get($this->_config['session_key'], $default);
	}

	protected function _createToken ($user)
	{
		$token = sha1($user['username'], openssl_random_pseudo_bytes(5));

		return $token;
	}

	protected function _hashToken ($token)
	{
		$hash = sha1($token);
		return $hash;
	}

	/**
	 * Complete the login for a user by incrementing the logins and setting
	 * session data: user_id, username
	 *
	 * @param	string		username
	 * @param	bool		remember
	 * @return 	void
	 */
	public function complete_login($user, $remember=false)
	{
		try
		{
			// update user information
			$query = array('username'	=>	$user['username']);
			$updates = array ('$set' => array('lastlogin'	=>	time() , 'logins' => $user['logins'] + 1));
			$result = $this->db->update($this->_config['table_name'], $query, $updates);

			if ($result === TRUE)
			{
				if ($remember)
				{
					// generate token to store in cookie for remember me function
					$token = $this->_createToken($user);

					Cookie::set('ycmdautotoken', $token, 86400*30);

					$this->db->put('user_tokens', array('item' => array(
						'username'	=>	$user['username'],
						'token'		=>	$this->_hashToken($token)
					)));
				}

				// Regenerate session_id
				$this->_session->regenerate();

				// Store username in session
				return $this->_session->set($this->_config['session_key'], $user);
			}
			else {
				return FALSE;
			}
		}
		catch (Exception $e)
		{
			var_dump($e);
			return FALSE;
		}
	}
} // End Auth NoSQL Mongo
