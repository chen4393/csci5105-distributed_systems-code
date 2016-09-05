

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
* This class defines a static method hash() which used the MD5 algorithm to hash a string to a value between 
* 0 and (MAX_NODES - 1) 
*/

public class HashService {

	public static int hash(String toHash){
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		digest.reset();
			
		digest.update(toHash.getBytes());
		byte[] array = digest.digest();
		
		int hashValue =	Math.abs(new BigInteger(array).intValue() % Constants.MAX_NODES.getValue());
		return hashValue;			
	}

}
