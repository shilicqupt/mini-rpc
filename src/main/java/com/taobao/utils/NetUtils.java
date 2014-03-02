package com.taobao.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by shili on 14-2-27.
 */
public class NetUtils {
    private static final Log LOG = LogFactory.getLog(NetUtils.class);

    private static Map<String, String> hostToResolved =
            new HashMap<String, String>();

    public static SocketFactory getSocketFactory() {

        SocketFactory factory = null;

        factory = SocketFactory.getDefault();

        return factory;
    }

    /**
     * Util method to build socket addr from either:
     *   <host>:<post>
     *   <fs>://<host>:<port>/<path>
     */
    public static InetSocketAddress createSocketAddr(String target) {
        return createSocketAddr(target, -1);
    }

    /**
     * Util method to build socket addr from either:
     *   <host>
     *   <host>:<post>
     *   <fs>://<host>:<port>/<path>
     */
    public static InetSocketAddress createSocketAddr(String target,
                                                     int defaultPort) {
        if (target == null) {
            throw new IllegalArgumentException("Socket address is null");
        }
        boolean hasScheme = target.contains("://");
        URI uri = null;
        try {
            uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://"+target);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Does not contain a valid host:port authority: " + target
            );
        }

        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            port = defaultPort;
        }
        String path = uri.getPath();

        if ((host == null) || (port < 0) ||
                (!hasScheme && path != null && !path.isEmpty()))
        {
            throw new IllegalArgumentException(
                    "Does not contain a valid host:port authority: " + target
            );
        }
        return makeSocketAddr(host, port);
    }

    public static InetSocketAddress makeSocketAddr(String host, int port) {

        InetSocketAddress addr = new InetSocketAddress(host, port);
        return addr;
    }

    /**
     * Resolve the uri's hostname and add the default port if not in the uri
     * @param uri to resolve
     * @param defaultPort if none is given
     * @return URI
     * @throws UnknownHostException
     */
    public static URI getCanonicalUri(URI uri, int defaultPort) {
        // skip if there is no authority, ie. "file" scheme or relative uri
        String host = uri.getHost();
        if (host == null) {
            return uri;
        }
        String fqHost = canonicalizeHost(host);
        int port = uri.getPort();
        // short out if already canonical with a port
        if (host.equals(fqHost) && port != -1) {
            return uri;
        }
        // reconstruct the uri with the canonical host and port
        try {
            uri = new URI(uri.getScheme(), uri.getUserInfo(),
                    fqHost, (port == -1) ? defaultPort : port,
                    uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return uri;
    }

    // cache the canonicalized hostnames;  the cache currently isn't expired,
    // but the canonicals will only change if the host's resolver configuration
    // changes
    private static ConcurrentHashMap<String, String> canonicalizedHostCache =
            new ConcurrentHashMap<String, String>();

    private static String canonicalizeHost(String host) {
        // check if the host has already been canonicalized
        String fqHost = canonicalizedHostCache.get(host);
        if (fqHost == null) {
            canonicalizedHostCache.put(host, fqHost);
        }
        return fqHost;
    }


    /**
     * Adds a static resolution for host. This can be used for setting up
     * hostnames with names that are fake to point to a well known host. For e.g.
     * in some testcases we require to have daemons with different hostnames
     * running on the same machine. In order to create connections to these
     * daemons, one can set up mappings from those hostnames to "localhost".
     * {@link NetUtils#getStaticResolution(String)} can be used to query for
     * the actual hostname.
     * @param host
     * @param resolvedName
     */
    public static void addStaticResolution(String host, String resolvedName) {
        synchronized (hostToResolved) {
            hostToResolved.put(host, resolvedName);
        }
    }

    /**
     * Retrieves the resolved name for the passed host. The resolved name must
     * have been set earlier using
     * {@link NetUtils#addStaticResolution(String, String)}
     * @param host
     * @return the resolution
     */
    public static String getStaticResolution(String host) {
        synchronized (hostToResolved) {
            return hostToResolved.get(host);
        }
    }

    /**
     * This is used to get all the resolutions that were added using
     * {@link NetUtils#addStaticResolution(String, String)}. The return
     * value is a List each element of which contains an array of String
     * of the form String[0]=hostname, String[1]=resolved-hostname
     * @return the list of resolutions
     */
    public static List<String[]> getAllStaticResolutions() {
        synchronized (hostToResolved) {
            Set<Map.Entry<String, String>> entries = hostToResolved.entrySet();
            if (entries.size() == 0) {
                return null;
            }
            List <String[]> l = new ArrayList<String[]>(entries.size());
            for (Map.Entry<String, String> e : entries) {
                l.add(new String[] {e.getKey(), e.getValue()});
            }
            return l;
        }
    }

    /**
     * Returns InetSocketAddress that a client can use to
     * connect to the server. Server.getListenerAddress() is not correct when
     * the server binds to "0.0.0.0". This returns "127.0.0.1:port" when
     * the getListenerAddress() returns "0.0.0.0:port".
     *
     * @param server
     * @return socket address that a client can use to connect to the server.
     */

    /**
     * TODO: reuse it when server is ok
     */
//    public static InetSocketAddress getConnectAddress(Server server) {
//        InetSocketAddress addr = server.getListenerAddress();
//        if (addr.getAddress().isAnyLocalAddress()) {
//            addr = makeSocketAddr("127.0.0.1", addr.getPort());
//        }
//        return addr;
//    }

    /**
     * Same as getInputStream(socket, socket.getSoTimeout()).<br><br>
     *
     * From documentation for {@link #getInputStream(Socket, long)}:<br>
     * Returns InputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketInputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getInputStream()} is returned. In the later
     * case, the timeout argument is ignored and the timeout set with
     * {@link Socket#setSoTimeout(int)} applies for reads.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getInputStream()}.
     *
     * @see #getInputStream(Socket, long)
     *
     * @param socket
     * @return InputStream for reading from the socket.
     * @throws java.io.IOException
     */
    public static InputStream getInputStream(Socket socket)
            throws IOException {
        return getInputStream(socket, socket.getSoTimeout());
    }

    /**
     * Returns InputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketInputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getInputStream()} is returned. In the later
     * case, the timeout argument is ignored and the timeout set with
     * {@link Socket#setSoTimeout(int)} applies for reads.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getInputStream()}.
     *
     * @see Socket#getChannel()
     *
     * @param socket
     * @param timeout timeout in milliseconds. This may not always apply. zero
     *        for waiting as long as necessary.
     * @return InputStream for reading from the socket.
     * @throws IOException
     */
    public static InputStream getInputStream(Socket socket, long timeout)
            throws IOException {
        return (socket.getChannel() == null) ?
                socket.getInputStream() : new SocketInputStream(socket, timeout);
    }

    /**
     * Same as getOutputStream(socket, 0). Timeout of zero implies write will
     * wait until data is available.<br><br>
     *
     * From documentation for {@link #getOutputStream(Socket, long)} : <br>
     * Returns OutputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketOutputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getOutputStream()} is returned. In the later
     * case, the timeout argument is ignored and the write will wait until
     * data is available.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getOutputStream()}.
     *
     * @see #getOutputStream(Socket, long)
     *
     * @param socket
     * @return OutputStream for writing to the socket.
     * @throws IOException
     */
    public static OutputStream getOutputStream(Socket socket)
            throws IOException {
        return getOutputStream(socket, 0);
    }

    /**
     * Returns OutputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketOutputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getOutputStream()} is returned. In the later
     * case, the timeout argument is ignored and the write will wait until
     * data is available.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getOutputStream()}.
     *
     * @see Socket#getChannel()
     *
     * @param socket
     * @param timeout timeout in milliseconds. This may not always apply. zero
     *        for waiting as long as necessary.
     * @return OutputStream for writing to the socket.
     * @throws IOException
     */
    public static OutputStream getOutputStream(Socket socket, long timeout)
            throws IOException {
        return (socket.getChannel() == null) ?
                socket.getOutputStream() : new SocketOutputStream(socket, timeout);
    }

    /**
     * This is a drop-in replacement for
     * {@link Socket#connect(SocketAddress, int)}.
     * In the case of normal sockets that don't have associated channels, this
     * just invokes <code>socket.connect(endpoint, timeout)</code>. If
     * <code>socket.getChannel()</code> returns a non-null channel,
     * connect is implemented using Hadoop's selectors. This is done mainly
     * to avoid Sun's connect implementation from creating thread-local
     * selectors, since Hadoop does not have control on when these are closed
     * and could end up taking all the available file descriptors.
     *
     * @see java.net.Socket#connect(java.net.SocketAddress, int)
     *
     * @param socket
     * @param address the remote address
     * @param timeout timeout in milliseconds
     */
    public static void connect(Socket socket,
                               SocketAddress address,
                               int timeout) throws IOException {
        connect(socket, address, null, timeout);
    }

    /**
     * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
     * also takes a local address and port to bind the socket to.
     *
     * @param socket
     * @param endpoint the remote address
     * @param localAddr the local address to bind the socket to
     * @param timeout timeout in milliseconds
     */
    public static void connect(Socket socket,
                               SocketAddress endpoint,
                               SocketAddress localAddr,
                               int timeout) throws IOException {
        if (socket == null || endpoint == null || timeout < 0) {
            throw new IllegalArgumentException("Illegal argument for connect()");
        }

        SocketChannel ch = socket.getChannel();

        if (localAddr != null) {
            socket.bind(localAddr);
        }

        if (ch == null) {
            // let the default implementation handle it.
            socket.connect(endpoint, timeout);
        } else {
            SocketIOWithTimeout.connect(ch, endpoint, timeout);
        }

        // There is a very rare case allowed by the TCP specification, such that
        // if we are trying to connect to an endpoint on the local machine,
        // and we end up choosing an ephemeral port equal to the destination port,
        // we will actually end up getting connected to ourself (ie any data we
        // send just comes right back). This is only possible if the target
        // daemon is down, so we'll treat it like connection refused.
        if (socket.getLocalPort() == socket.getPort() &&
                socket.getLocalAddress().equals(socket.getInetAddress())) {
            LOG.info("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException(
                    "Localhost targeted connection resulted in a loopback. " +
                            "No daemon is listening on the target port.");
        }
    }

    /**
     * Given a string representation of a host, return its ip address
     * in textual presentation.
     *
     * @param name a string representation of a host:
     *             either a textual representation its IP address or its host name
     * @return its IP address in the string format
     */
    public static String normalizeHostName(String name) {
        try {
            InetAddress ipAddress = InetAddress.getByName(name);
            return ipAddress.getHostAddress();
        } catch (UnknownHostException e) {
            return name;
        }
    }

    /**
     * Given a collection of string representation of hosts, return a list of
     * corresponding IP addresses in the textual representation.
     *
     * @param names a collection of string representations of hosts
     * @return a list of corresponding IP addresses in the string format
     * @see #normalizeHostName(String)
     */
    public static List<String> normalizeHostNames(Collection<String> names) {
        List<String> hostNames = new ArrayList<String>(names.size());
        for (String name : names) {
            hostNames.add(normalizeHostName(name));
        }
        return hostNames;
    }

    /**
     * Performs a sanity check on the list of hostnames/IPs to verify they at least
     * appear to be valid.
     * @param names - List of hostnames/IPs
     * @throws UnknownHostException
     */
    public static void verifyHostnames(String[] names) throws UnknownHostException {
        for (String name: names) {
            if (name == null) {
                throw new UnknownHostException("null hostname found");
            }
            // The first check supports URL formats (e.g. hdfs://, etc.).
            // java.net.URI requires a schema, so we add a dummy one if it doesn't
            // have one already.
            URI uri = null;
            try {
                uri = new URI(name);
                if (uri.getHost() == null) {
                    uri = new URI("http://" + name);
                }
            } catch (URISyntaxException e) {
                uri = null;
            }
            if (uri == null || uri.getHost() == null) {
                throw new UnknownHostException(name + " is not a valid Inet address");
            }
        }
    }

    /**
     * Checks if {@code host} is a local host name and return {@link InetAddress}
     * corresponding to that address.
     *
     * @param host the specified host
     * @return a valid local {@link InetAddress} or null
     * @throws SocketException if an I/O error occurs
     */
    public static InetAddress getLocalInetAddress(String host)
            throws SocketException {
        if (host == null) {
            return null;
        }
        InetAddress addr = null;
        try {
            addr = InetAddress.getByName(host);
            if (NetworkInterface.getByInetAddress(addr) == null) {
                addr = null; // Not a local address
            }
        } catch (UnknownHostException ignore) { }
        return addr;
    }

    /**
     * @return true if the given string is a subnet specified
     *     using CIDR notation, false otherwise
     */
    public static boolean isValidSubnet(String subnet) {
        try {
            new SubnetUtils(subnet);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    /**
     * Add all addresses associated with the given nif in the
     * given subnet to the given list.
     */
    private static void addMatchingAddrs(NetworkInterface nif,
                                         SubnetUtils.SubnetInfo subnetInfo, List<InetAddress> addrs) {
        Enumeration<InetAddress> ifAddrs = nif.getInetAddresses();
        while (ifAddrs.hasMoreElements()) {
            InetAddress ifAddr = ifAddrs.nextElement();
            if (subnetInfo.isInRange(ifAddr.getHostAddress())) {
                addrs.add(ifAddr);
            }
        }
    }

    /**
     * Return an InetAddress for each interface that matches the
     * given subnet specified using CIDR notation.
     *
     * @param subnet subnet specified using CIDR notation
     * @param returnSubinterfaces
     *            whether to return IPs associated with subinterfaces
     * @throws IllegalArgumentException if subnet is invalid
     */
    public static List<InetAddress> getIPs(String subnet,
                                           boolean returnSubinterfaces) {
        List<InetAddress> addrs = new ArrayList<InetAddress>();
        SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(subnet).getInfo();
        Enumeration<NetworkInterface> nifs;

        try {
            nifs = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            LOG.error("Unable to get host interfaces", e);
            return addrs;
        }

        while (nifs.hasMoreElements()) {
            NetworkInterface nif = nifs.nextElement();
            // NB: adding addresses even if the nif is not up
            addMatchingAddrs(nif, subnetInfo, addrs);

            if (!returnSubinterfaces) {
                continue;
            }
            Enumeration<NetworkInterface> subNifs = nif.getSubInterfaces();
            while (subNifs.hasMoreElements()) {
                addMatchingAddrs(subNifs.nextElement(), subnetInfo, addrs);
            }
        }
        return addrs;
    }
}
