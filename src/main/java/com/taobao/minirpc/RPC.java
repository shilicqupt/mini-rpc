package com.taobao.minirpc;

import com.taobao.utils.Configuration;
import com.taobao.utils.NetUtils;
import com.taobao.utils.ObjectWritable;
import com.taobao.utils.UTF8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shili on 14-2-27.
 */
public class RPC {
    private static final Log LOG = LogFactory.getLog(RPC.class);

    private RPC() {}                                  // no public ctor


    /** A method invocation, including the method name and its parameters.*/
    private static class Invocation implements Writable, Configurable {
        private String methodName;
        private Class[] parameterClasses;
        private Object[] parameters;
        private Configuration conf;

        public Invocation() {}

        public Invocation(Method method, Object[] parameters) {
            this.methodName = method.getName();
            this.parameterClasses = method.getParameterTypes();
            this.parameters = parameters;
        }

        /** The name of the method invoked. */
        public String getMethodName() { return methodName; }

        /** The parameter classes. */
        public Class[] getParameterClasses() { return parameterClasses; }

        /** The parameter instances. */
        public Object[] getParameters() { return parameters; }

        public void readFields(DataInput in) throws IOException {
            methodName = UTF8.readString(in);
            parameters = new Object[in.readInt()];
            parameterClasses = new Class[parameters.length];
            ObjectWritable objectWritable = new ObjectWritable();
            for (int i = 0; i < parameters.length; i++) {
                parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
                parameterClasses[i] = objectWritable.getDeclaredClass();
            }
        }

        public void write(DataOutput out) throws IOException {
            UTF8.writeString(out, methodName);
            out.writeInt(parameterClasses.length);
            for (int i = 0; i < parameterClasses.length; i++) {
                ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                        conf);
            }
        }

        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append(methodName);
            buffer.append("(");
            for (int i = 0; i < parameters.length; i++) {
                if (i != 0)
                    buffer.append(", ");
                buffer.append(parameters[i]);
            }
            buffer.append(")");
            return buffer.toString();
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        public Configuration getConf() {
            return this.conf;
        }

    }

    /* Cache a client using its socket factory as the hash key */
    static private class ClientCache {
        private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

        /**
         * Construct & cache an IPC client with the user-provided SocketFactory
         * if no cached client exists.
         *
         * @return an IPC client
         */
        private synchronized Client getClient(SocketFactory factory) {
            // Construct & cache client.  The configuration is only used for timeout,
            // and Clients have connection pools.  So we can either (a) lose some
            // connection pooling and leak sockets, or (b) use the same timeout for all
            // configurations.  Since the IPC is usually intended globally, not
            // per-job, we choose (a).
            Client client = clients.get(factory);
            if (client == null) {
                client = new Client(ObjectWritable.class);
                clients.put(factory, client);
            } else {
                client.incCount();
            }
            return client;
        }

        /**
         * Construct & cache an IPC client with the default SocketFactory
         * if no cached client exists.
         *
         * @return an IPC client
         */
        private synchronized Client getClient() {
            return getClient(SocketFactory.getDefault());
        }

        /**
         * Stop a RPC client connection
         * A RPC client is closed only when its reference count becomes zero.
         */
        private void stopClient(Client client) {
            synchronized (this) {
                client.decCount();
                if (client.isZeroReference()) {
                    clients.remove(client.getSocketFactory());
                }
            }
            if (client.isZeroReference()) {
                client.stop();
            }
        }
    }

    private static ClientCache CLIENTS=new ClientCache();

    //for unit testing only
    static Client getClient() {
        return CLIENTS.getClient();
    }

    private static class Invoker implements InvocationHandler {
        private Client.ConnectionId remoteId;
        private Client client;
        private boolean isClosed = false;

        private Invoker(Class<? extends VersionedProtocol> protocol,
                        InetSocketAddress address, SocketFactory factory,
                        int rpcTimeout, RetryPolicy connectionRetryPolicy) throws IOException {
            this.remoteId = Client.ConnectionId.getConnectionId(address, protocol, rpcTimeout, connectionRetryPolicy);
            this.client = CLIENTS.getClient(factory);
        }

        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            final boolean logDebug = LOG.isDebugEnabled();
            long startTime = 0;
            if (logDebug) {
                startTime = System.currentTimeMillis();
            }

            ObjectWritable value = (ObjectWritable)
                    client.call(new Invocation(method, args), remoteId);   //无需传入协议名称只需要方法名称和参数，因为协议名称已经由ConnectionHeader传入到Server
            if (logDebug) {
                long callTime = System.currentTimeMillis() - startTime;
                LOG.debug("Call: " + method.getName() + " " + callTime);
            }
            return value.get();
        }

        /* close the IPC client that's responsible for this invoker's RPCs */
        synchronized private void close() {
            if (!isClosed) {
                isClosed = true;
                CLIENTS.stopClient(client);
            }
        }
    }

    /**
     * A version mismatch for the RPC protocol.
     */
    public static class VersionMismatch extends IOException {
        private String interfaceName;
        private long clientVersion;
        private long serverVersion;

        /**
         * Create a version mismatch exception
         * @param interfaceName the name of the protocol mismatch
         * @param clientVersion the client's version of the protocol
         * @param serverVersion the server's version of the protocol
         */
        public VersionMismatch(String interfaceName, long clientVersion,
                               long serverVersion) {
            super("Protocol " + interfaceName + " version mismatch. (client = " +
                    clientVersion + ", server = " + serverVersion + ")");
            this.interfaceName = interfaceName;
            this.clientVersion = clientVersion;
            this.serverVersion = serverVersion;
        }

        /**
         * Get the interface name
         * @return the java class name
         *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
         */
        public String getInterfaceName() {
            return interfaceName;
        }

        /**
         * Get the client's preferred version
         */
        public long getClientVersion() {
            return clientVersion;
        }

        /**
         * Get the server's agreed to version.
         */
        public long getServerVersion() {
            return serverVersion;
        }
    }

    public static VersionedProtocol waitForProxy(
            Class<? extends VersionedProtocol> protocol,
            long clientVersion,
            InetSocketAddress addr,
            Configuration conf
    ) throws IOException {
        return waitForProxy(protocol, clientVersion, addr, conf, 0, Long.MAX_VALUE);
    }

    /**
     * Get a proxy connection to a remote server
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param connTimeout time in milliseconds before giving up
     * @return the proxy
     * @throws IOException if the far end through a RemoteException
     */
    static VersionedProtocol waitForProxy(
            Class<? extends VersionedProtocol> protocol,
            long clientVersion,
            InetSocketAddress addr,
            Configuration conf,
            long connTimeout)
            throws IOException {
        return waitForProxy(protocol, clientVersion, addr, conf, 0, connTimeout);
    }

    static VersionedProtocol waitForProxy(
            Class<? extends VersionedProtocol> protocol,
            long clientVersion,
            InetSocketAddress addr,
            Configuration conf,
            int rpcTimeout,
            long connTimeout)
            throws IOException {
        long startTime = System.currentTimeMillis();
        IOException ioe;
        while (true) {
            try {
                return getProxy(protocol, clientVersion, addr, rpcTimeout);
            } catch(ConnectException se) {  // namenode has not been started
                LOG.info("Server at " + addr + " not available yet, Zzzzz...");
                ioe = se;
            } catch(SocketTimeoutException te) {  // namenode is busy
                LOG.info("Problem connecting to server: " + addr);
                ioe = te;
            }
            // check if timed out
            if (System.currentTimeMillis()-connTimeout >= startTime) {
                throw ioe;
            }

            // wait for retry
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // IGNORE
            }
        }
    }

    public static VersionedProtocol getProxy(
            Class<? extends VersionedProtocol> protocol,
            long clientVersion, InetSocketAddress addr, SocketFactory factory) throws IOException {
        return getProxy(protocol, clientVersion, addr, factory, 0);
    }

    public static VersionedProtocol getProxy(
            Class<? extends VersionedProtocol> protocol,
            long clientVersion, InetSocketAddress addr,
            SocketFactory factory, int rpcTimeout) throws IOException {
        return getProxy(protocol, clientVersion, addr, factory,
                rpcTimeout, null, true);
    }

    /** Construct a client-side proxy object that implements the named protocol,
     * talking to a server at the named address. */
    public static VersionedProtocol getProxy(
            Class<? extends VersionedProtocol> protocol,   //协议接口，必须实现接口VersionedProtocol
            long clientVersion, InetSocketAddress addr, SocketFactory factory, int rpcTimeout,
            RetryPolicy connectionRetryPolicy,   //可拔插的重试机制
            boolean checkVersion) throws IOException {

        final Invoker invoker = new Invoker(protocol, addr, factory,
                rpcTimeout, connectionRetryPolicy);
        VersionedProtocol proxy = (VersionedProtocol) Proxy.newProxyInstance(
                protocol.getClassLoader(), new Class[]{protocol}, invoker);

//        if (checkVersion) {
//            checkVersion(protocol, clientVersion, proxy);
//        }
        return proxy;
    }

    /** Get server version and then compare it with client version. */
    public static void checkVersion(Class<? extends VersionedProtocol> protocol,
                                    long clientVersion, VersionedProtocol proxy)  throws IOException {
        long serverVersion = proxy.getProtocolVersion(protocol.getName(),
                clientVersion);
        if (serverVersion != clientVersion) {
            throw new VersionMismatch(protocol.getName(), clientVersion,
                    serverVersion);
        }
    }

    public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
            long clientVersion, InetSocketAddress addr) throws IOException {
        return getProxy(protocol, clientVersion, addr, NetUtils.getSocketFactory(), 0);
    }

    public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
            long clientVersion, InetSocketAddress addr, int rpcTimeout) throws IOException {
        return getProxy(protocol, clientVersion, addr, NetUtils.getSocketFactory(), rpcTimeout);
    }

    /**
     * Stop this proxy and release its invoker's resource
     * @param proxy the proxy to be stopped
     */
    public static void stopProxy(VersionedProtocol proxy) {
        if (proxy!=null) {
            ((Invoker)Proxy.getInvocationHandler(proxy)).close();
        }
    }

    /** Expert: Make multiple, parallel calls to a set of servers. */
    public static Object[] call(Method method, Object[][] params,
                                InetSocketAddress[] addrs)
            throws IOException, InterruptedException {

        Invocation[] invocations = new Invocation[params.length];
        for (int i = 0; i < params.length; i++)
            invocations[i] = new Invocation(method, params[i]);
        Client client = CLIENTS.getClient();
        try {
            Writable[] wrappedValues =
                    client.call(invocations, addrs, method.getDeclaringClass());

            if (method.getReturnType() == Void.TYPE) {
                return null;
            }

            Object[] values =
                    (Object[]) Array.newInstance(method.getReturnType(), wrappedValues.length);
            for (int i = 0; i < values.length; i++)
                if (wrappedValues[i] != null)
                    values[i] = ((ObjectWritable)wrappedValues[i]).get();

            return values;
        } finally {
            CLIENTS.stopClient(client);
        }
    }

    /** Construct a server for a protocol implementation instance listening on a
     * port and address. */
    public static Server getServer(final Object instance, final String bindAddress, final int port)
            throws IOException {
        return getServer(instance, bindAddress, port, 1, false);
    }

    /** Construct a server for a protocol implementation instance listening on a
     * port and address. */
    public static Server getServer(final Object instance, final String bindAddress, final int port,
                                   final int numHandlers,
                                   final boolean verbose)
            throws IOException {
        return new Server(instance, bindAddress, port, numHandlers, verbose);
    }

    /** An RPC Server. */
    public static class Server extends com.taobao.minirpc.Server {
        private Object instance;
        private boolean verbose;

        /** Construct an RPC server.
         * @param instance the instance whose methods will be called
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         */
        public Server(Object instance, String bindAddress, int port)
                throws IOException {
            this(instance, bindAddress, port, 1, false);
        }

        private static String classNameBase(String className) {
            String[] names = className.split("\\.", -1);
            if (names == null || names.length == 0) {
                return className;
            }
            return names[names.length-1];
        }

        /** Construct an RPC server.
         * @param instance the instance whose methods will be called
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         * @param numHandlers the number of method handler threads to run
         * @param verbose whether each call should be logged
         */
        public Server(Object instance, String bindAddress,  int port,
                      int numHandlers, boolean verbose)
                throws IOException {
            super(bindAddress, port, Invocation.class, numHandlers,
                    classNameBase(instance.getClass().getName()));
            this.instance = instance;
            this.verbose = verbose;
        }

        public Writable call(Class<?> protocol, Writable param, long receivedTime)
                throws IOException {
            try {
                Invocation call = (Invocation)param;
                if (verbose) log("Call: " + call);

                Method method =
                        protocol.getMethod(call.getMethodName(),
                                call.getParameterClasses());
                method.setAccessible(true);

                long startTime = System.currentTimeMillis();
                Object value = method.invoke(instance, call.getParameters());
                int processingTime = (int) (System.currentTimeMillis() - startTime);
                int qTime = (int) (startTime-receivedTime);
                LOG.debug("****Served: " + call.getMethodName() +
                            " queueTime= " + qTime + " procesingTime= " + processingTime + "***");
                if (verbose) log("Return: "+value);

                return new ObjectWritable(method.getReturnType(), value);

            } catch (InvocationTargetException e) {
                Throwable target = e.getTargetException();
                if (target instanceof IOException) {
                    throw (IOException)target;
                } else {
                    IOException ioe = new IOException(target.toString());
                    ioe.setStackTrace(target.getStackTrace());
                    throw ioe;
                }
            } catch (Throwable e) {
                if (!(e instanceof IOException)) {
                    LOG.error("Unexpected throwable object ", e);
                }
                IOException ioe = new IOException(e.toString());
                ioe.setStackTrace(e.getStackTrace());
                throw ioe;
            }
        }
    }

    private static void log(String value) {
        if (value!= null && value.length() > 55)
            value = value.substring(0, 55)+"...";
        LOG.info(value);
    }
}
