package org.ccjmne.faomaintenance.api.rest;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Path("test")
@Singleton
public class CacheTestEndpoint {

	private static final Random R = new Random();
	private final LoadingCache<Integer, Integer> cache;

	public CacheTestEndpoint() {
		this.cache = CacheBuilder.newBuilder().refreshAfterWrite(10, TimeUnit.SECONDS).expireAfterAccess(60, TimeUnit.SECONDS)
				.<Integer, Integer> build(CacheLoader.asyncReloading(CacheLoader.from(val -> {
					try {
						Thread.sleep(5000);
					} catch (final Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return Integer.valueOf(R.nextInt());
				}), r -> new Thread(r).start()));

		new Thread(() -> {
			int i = 0;
			for (;;) {
				System.out.println(i++);
				try {
					Thread.sleep(1000);
				} catch (final Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	@GET
	public int test() {
		final int res = this.cache.getUnchecked(Integer.valueOf(0)).intValue();
		System.out.println(res);
		return res;
	}
}
