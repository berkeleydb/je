/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.rep.utilint;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;
import sun.net.spi.nameservice.NameService;
import sun.net.spi.nameservice.NameServiceDescriptor;

import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Define a JDK name service provider that can be controlled by tests to
 * simulate DNS failures.  The idea is to define dummy DNS names that translate
 * to the loopback address, and then undefine them as needed.
 *
 * <p>To use this class, you need to make a few modifications to the JVM that
 * wants to use it: <ul>
 *
 * <li>Tell the JDK how to find this service provider by creating a resource,
 * available in the class path, named
 * <code>META-INF/services/sun.net.spi.nameservice.NameService</code>, which
 * contains the fully qualified name of the {@link Descriptor} class.  You can
 * do that by create a file with that pathname relative to a component of the
 * classpath.
 *
 * <li>Add the new name service provider as a second provider after the
 * standard provider by setting the following system properties:
 * <pre>
 * -Dsun.net.spi.nameservice.provider.1=dns,default
 * -Dsun.net.spi.nameservice.provider.2=dns,localalias
 * </pre>
 * Note that these properties need to be set on the command line so they are
 * available at JVM startup time.
 *
 * <li>Disable the DNS cache so that changes made to this provider will take
 * effect.  Do this by calling {@link #setDNSCachePolicy} with 0 for both cache
 * policies, and reverting to the original values afterwards.
 *
 * </ul>
 *
 * Although the name service provider facility is undocumented, at last check
 * it appears to be supported by the J9 and icedtea JVM implementations.
 */
public class LocalAliasNameService implements NameService {

    static final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(LocalAliasNameService.class, "Test");

    /* Only referenced by getLocalHost */
    private static InetAddress localHost = null;
    private static boolean computingLocalHost = false;

    /**
     * The service descriptor that defines {@code LocalAliasNameService} as a
     * "dns" provider named "localalias".
     */
    public static class Descriptor implements NameServiceDescriptor {

        @Override
        public NameService createNameService() {
            return new LocalAliasNameService();
        }
        @Override
        public String getProviderName() {
            return "localalias";
        }
        @Override
        public String getType() {
            return "dns";
        }
    }

    private static final Set<String> aliases =
        Collections.synchronizedSet(new HashSet<String>());

    private LocalAliasNameService() {
        logger.info("Created LocalAliasNameService");
    }

    /**
     * Add a new alias for the loopback address.
     *
     * @param alias the new alias
     */
    public static void addAlias(String alias) {
        logger.info("LocalAliasNameService.addAlias: " + alias);
        aliases.add(alias);
    }

    /**
     * Remove an alias for the loopback address.
     *
     * @param alias the alias to remove
     */
    public static void removeAlias(String alias) {
        logger.info("LocalAliasNameService.removeAlias: " + alias);
        aliases.remove(alias);
    }

    /**
     * Remove all aliases for the loopback address.
     */
    public static void clearAllAliases() {
        logger.info("LocalAliasNameService.clearAllAliases");
        aliases.clear();
    }

    /**
     * Use reflection to set the internal DNS cache policy.  The {@link
     * InetAddress} class documents security properties for controlling this
     * (networkaddress.cache.ttl and networkaddress.cache.negative.ttl), but
     * those settings only take effect when they are specified before any
     * address lookups are performed.  The JUnit test infrastructure does
     * address lookups before testing starts, so the security properties are
     * not effective.  Instead, use reflection to set the field values
     * directly.
     *
     * <p>The cache policy values specify the time in milliseconds that the
     * cache should remain valid, with 0 meaning don't cache and -1 meaning
     * cache stays valid forever.
     *
     * @param positive the cache policy for successful lookups
     * @param negative the cache policy for unsuccessful lookups
     * @return an array of the previous cache policies, with the positive cache
     * value appearing first
     */
    public static int[] setDNSCachePolicy(int positive, int negative) {
        try {
            Class<?> cachePolicyClass
                = Class.forName("sun.net.InetAddressCachePolicy");
            Field positiveField =
                cachePolicyClass.getDeclaredField("cachePolicy");
            positiveField.setAccessible(true);
            Field negativeField =
                cachePolicyClass.getDeclaredField("negativeCachePolicy");
            negativeField.setAccessible(true);
            int[] result = new int[2];
            synchronized (cachePolicyClass) {
                result[0] = positiveField.getInt(null);
                result[1] = negativeField.getInt(null);
                positiveField.setInt(null, positive);
                negativeField.setInt(null, negative);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(
                "Unexpected exception when setting DNS cache: " + e, e);
        }
    }

    /**
     * This implementation returns the loopback address for hosts that match a
     * current alias.
     */
    @Override
    public InetAddress[] lookupAllHostAddr(String host)
        throws UnknownHostException {

        if (!aliases.contains(host)) {
            logger.info("LocalAliasNameService.lookupAllHostAddr:" +
                        " Unknown host: " + host);
            throw new UnknownHostException("Unknown host: " + host);
        }
        final InetAddress lh = getLocalHost();
        logger.info("LocalAliasNameService.lookupAllHostAddr:" + host +
                    " => " + lh);
        return new InetAddress[] { lh };
    }

    /**
     * Compute the local host if needed, avoiding circularities.  The main name
     * service provider should provide the local host when called from
     * InetAddress.getLocalHost, so it should be OK to throw
     * UnknownHostException if this method is called recursively.
     */
    private static synchronized InetAddress getLocalHost()
        throws UnknownHostException {

        if (localHost == null) {
            if (computingLocalHost) {
                throw new UnknownHostException("Local host");
            }
            computingLocalHost = true;
            try {
                localHost = InetAddress.getLocalHost();
            } finally {
                computingLocalHost = false;
            }
        }
        return localHost;
    }

    /**
     * This implementation returns one of the current aliases if the argument
     * matches the loopback address and there is at least one alias.
     */
    @Override
    public String getHostByAddr(byte[] addr)
        throws UnknownHostException {

        final InetAddress inetAddr = InetAddress.getByAddress(addr);
        if (!getLocalHost().equals(inetAddr.getHostAddress())) {
            logger.info("LocalAliasNameService.getHostByAddr:" +
                        " No mapping for address");
            throw new UnknownHostException("No mapping for address");
        }
        synchronized (aliases) {
            final Iterator<String> iter = aliases.iterator();
            if (iter.hasNext()) {
                String hostname = iter.next();
                logger.info("LocalAliasNameService.getHostByAddr: " +
                            hostname);
                return hostname;
            }
        }
        logger.info("LocalAliasNameService.getHostByAddr:" +
                    " No mapping for address");
        throw new UnknownHostException("No mapping for address");
    }
}
