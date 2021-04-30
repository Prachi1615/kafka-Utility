/**
 * 
 */
package redis;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class RedisUtility {
	
	@Test
    public void test() throws IOException {
		
		RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
		client.setDefaultTimeout(20, TimeUnit.SECONDS);

		

		client.shutdown();
        
    }
    
}