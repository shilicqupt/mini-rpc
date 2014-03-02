package com.taobao.minirpc;

import com.taobao.utils.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by shili on 14-2-27.
 */
public class Client {

    public static final Log LOG = LogFactory.getLog(Client.class);
    private Hashtable<ConnectionId, Connection> connections =
            new Hashtable<ConnectionId, Connection>();

    private Class<? extends Writable> valueClass;   // class of call values
    private int counter;                            // counter for call ids
    private AtomicBoolean running = new AtomicBoolean(true); // if client runs

    private SocketFactory socketFactory;           // how to create sockets
    private int refCount = 1;

    final static int PING_CALL_ID = -1;

    /**
     * Get the ping interval;
     */
    final static int getPingInterval() {
        return 60000; //PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL 1����
    }

    /**
     * The time after which a RPC will timeout. If ping is not enabled (via
     * ipc.client.ping), then the timeout value is the same as the pingInterval.
     * If ping is enabled, then there is no timeout value.
     */
    final public static int getTimeout() {
        return -1;
    }

    /**
     * Increment this client's reference count
     *
     */
    synchronized void incCount() {
        refCount++;
    }

    /**
     * Decrement this client's reference count
     *
     */
    synchronized void decCount() {
        refCount--;
    }

    /**
     * Return if this client has no reference
     *
     * @return true if this client has no reference; false otherwise
     */
    synchronized boolean isZeroReference() {
        return refCount==0;
    }

    /** A call waiting for a value. */
    private class Call {
        int id;                                       // call id
        Writable param;                               // parameter  ����Զ�̷�������
        Writable value;                               // value, null if error   Զ�̷������صĽ�������Ϊ�շ�������
        IOException error;                            // exception, null if value   Զ�̵����׳��쳣������쳣
        boolean done;                                 // true when call is done    ��ʶԶ�̵����Ƿ����

        protected Call(Writable param) {
            this.param = param;
            synchronized (Client.this) {
                this.id = counter++;
            }
        }

        /** Indicate when the call is complete and the
         * value or error are available.  Notifies by default.  */
        protected synchronized void callComplete() {
            this.done = true;
            notify();                                 // notify caller
        }

        /** Set the exception when there is an error.
         * Notify the caller the call is done.
         *
         * @param error exception thrown by the call; either local or remote
         */
        public synchronized void setException(IOException error) {
            this.error = error;
            callComplete();
        }

        /** Set the return value when there is no error.
         * Notify the caller the call is done.
         *
         * @param value return value of the call.
         */
        public synchronized void setValue(Writable value) {
            this.value = value;
            callComplete();
        }
    }

    /** Thread that reads responses and notifies callers.  Each connection owns a
     * socket connected to a remote address.  Calls are multiplexed through this
     * socket: responses may be delivered out of order. */

    /**
     * �����̹߳���socket
     */
     private class Connection extends Thread {
        private InetSocketAddress server;             // server ip:port
        private ConnectionHeader header;              // connection header
        private final ConnectionId remoteId;                // connection id

        private Socket socket = null;                 // connected socket
        private DataInputStream in;
        private DataOutputStream out;
        private int rpcTimeout;
        private int maxIdleTime; //connections will be culled if it was idle for
        //maxIdleTime msecs
        private final RetryPolicy connectionRetryPolicy;
        private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
        private int pingInterval; // how often sends ping to the server in msecs


        // currently active calls
        private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
        private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
        private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
        private IOException closeException; // close reason

        public Connection(ConnectionId remoteId) throws IOException {
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            if (server.isUnresolved()) {
                throw new UnknownHostException("unknown host: " +
                        remoteId.getAddress().getHostName());
            }
            this.maxIdleTime = remoteId.getMaxIdleTime();
            this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
            this.tcpNoDelay = remoteId.getTcpNoDelay();
            this.pingInterval = remoteId.getPingInterval();
            if (LOG.isDebugEnabled()) {
                LOG.debug("The ping interval is" + this.pingInterval + "ms.");
            }
            this.rpcTimeout = remoteId.getRpcTimeout();
            Class<?> protocol = remoteId.getProtocol();

            header = new ConnectionHeader(protocol == null ? null : protocol.getName());

            this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
                    remoteId.getAddress().toString());
            this.setDaemon(true);
        }

        /** Update lastActivity with the current time. */
        private void touch() {
            lastActivity.set(System.currentTimeMillis());
        }

        /**
         * Add a call to this connection's call queue and notify
         * a listener; synchronized.
         * Returns false if called during shutdown.
         * @param call to add
         * @return true if the call was added.
         */
        private synchronized boolean addCall(Call call) {
            if (shouldCloseConnection.get())
                return false;
            calls.put(call.id, call);
            notify();
            return true;
        }

        /** This class sends a ping to the remote side when timeout on
         * reading. If no failure is detected, it retries until at least
         * a byte is read.
         */
        private class PingInputStream extends FilterInputStream {
            /* constructor */
            protected PingInputStream(InputStream in) {
                super(in);
            }

            /* Process timeout exception
             * if the connection is not going to be closed or
             * is not configured to have a RPC timeout, send a ping.
             * (if rpcTimeout is not set to be 0, then RPC should timeout.
             * otherwise, throw the timeout exception.
             */
            private void handleTimeout(SocketTimeoutException e) throws IOException {
                if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
                    throw e;
                } else {
                    sendPing();
                }
            }

            /** Read a byte from the stream.
             * Send a ping if timeout on read. Retries if no failure is detected
             * until a byte is read.
             * @throws IOException for any IO problem other than socket timeout
             */
            public int read() throws IOException {
                do {
                    try {
                        return super.read();
                    } catch (SocketTimeoutException e) {
                        handleTimeout(e);
                    }
                } while (true);
            }

            /** Read bytes into a buffer starting from offset <code>off</code>
             * Send a ping if timeout on read. Retries if no failure is detected
             * until a byte is read.
             *
             * @return the total number of bytes read; -1 if the connection is closed.
             */
            public int read(byte[] buf, int off, int len) throws IOException {
                do {
                    try {
                        return super.read(buf, off, len);
                    } catch (SocketTimeoutException e) {
                        handleTimeout(e);
                    }
                } while (true);
            }
        }

        /**
         * Update the server address if the address corresponding to the host
         * name has changed.
         *
         * @return true if an addr change was detected.
         * @throws IOException when the hostname cannot be resolved.
         */
        private synchronized boolean updateAddress() throws IOException {
            // Do a fresh lookup with the old host name.
            InetSocketAddress currentAddr = NetUtils.makeSocketAddr(
                    server.getHostName(), server.getPort());

            if (!server.equals(currentAddr)) {
                LOG.warn("Address change detected. Old: " + server.toString() +
                        " New: " + currentAddr.toString());
                server = currentAddr;
                return true;
            }
            return false;
        }

        /**
         * ��server�������г�ʱTCP����
         * @throws IOException
         */
        private synchronized void setupConnection() throws IOException {
            short ioFailures = 0;
            short timeoutFailures = 0;
            while (true) {
                try {
                    this.socket = socketFactory.createSocket();
                    this.socket.setTcpNoDelay(tcpNoDelay);

          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */

                    // connection time out is 20s
                    NetUtils.connect(this.socket, server, 20000);
                    if (rpcTimeout > 0) {
                        pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
                    }

                    this.socket.setSoTimeout(pingInterval);
                    return;
                } catch (SocketTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
                    if (updateAddress()) {
                        timeoutFailures = ioFailures = 0;
                    }
          /* The max number of retries is 45,
           * which amounts to 20s*45 = 15 minutes retries.
           */
                    handleConnectionFailure(timeoutFailures++, 45, toe);
                } catch (IOException ie) {
                    if (updateAddress()) {
                        timeoutFailures = ioFailures = 0;
                    }
                    handleConnectionFailure(ioFailures++, ie);
                }
            }
        }

        /** Connect to the server and set up the I/O streams. It then sends
         * a header to the server and starts
         * the connection thread that waits for responses.
         */
        private synchronized void setupIOstreams() throws InterruptedException {
            if (socket != null || shouldCloseConnection.get()) {
                return;
            }

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connecting to "+server);
                }
                short numRetries = 0;
                final short maxRetries = 15;
                Random rand = null;
                while (true) {
                    setupConnection();
                    InputStream inStream = NetUtils.getInputStream(socket);
                    OutputStream outStream = NetUtils.getOutputStream(socket);
                    /**
                     * дrpc header��server,����rpc��ʼ��"hrpc"+�汾��
                     */
                    writeRpcHeader(outStream);
                    this.in = new DataInputStream(new BufferedInputStream
                            (new PingInputStream(inStream)));
                    this.out = new DataOutputStream
                            (new BufferedOutputStream(outStream));
                    /**
                     * д����ͷConntionHeader ���䳤�Ƚ��н���
                     */
                    writeHeader();

                    // update last activity time
                    touch();

                    // start the receiver thread after the socket connection has been set up
                    start();
                    return;
                }
            } catch (Throwable t) {
                if (t instanceof IOException) {
                    markClosed((IOException)t);
                } else {
                    markClosed(new IOException("Couldn't set up IO streams", t));
                }
                close();
            }
        }

        private void closeConnection() {
            // close the current connection
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Not able to close a socket", e);
            }
            // set socket to null so that the next call to setupIOstreams
            // can start the process of connect all over again.
            socket = null;
        }

        /* Handle connection failures
         *
         * If the current number of retries is equal to the max number of retries,
         * stop retrying and throw the exception; Otherwise backoff 1 second and
         * try connecting again.
         *
         * This Method is only called from inside setupIOstreams(), which is
         * synchronized. Hence the sleep is synchronized; the locks will be retained.
         *
         * @param curRetries current number of retries
         * @param maxRetries max number of retries allowed
         * @param ioe failure reason
         * @throws IOException if max number of retries is reached
         */
        private void handleConnectionFailure(
                int curRetries, int maxRetries, IOException ioe) throws IOException {

            closeConnection();

            // throw the exception if the maximum number of retries is reached
            if (curRetries >= maxRetries) {
                throw ioe;
            }

            // otherwise back off and retry
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}

            LOG.info("Retrying connect to server: " + server + ". Already tried "
                    + curRetries + " time(s); maxRetries=" + maxRetries);
        }

        private void handleConnectionFailure(int curRetries, IOException ioe
        ) throws IOException {
            closeConnection();

            final boolean retry;
            try {
                retry = connectionRetryPolicy.shouldRetry(ioe, curRetries);
            } catch(Exception e) {
                throw e instanceof IOException? (IOException)e: new IOException(e);
            }
            if (!retry) {
                throw ioe;
            }

            LOG.info("Retrying connect to server: " + server + ". Already tried "
                    + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
        }

        /* Write the RPC header */
        private void writeRpcHeader(OutputStream outStream) throws IOException {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
            // Write out the header, version and authentication method
            out.write(Server.HEADER.array());
            out.write(Server.CURRENT_VERSION);
            out.flush();
        }

        /* Write the protocol header for each connection
         * Out is not synchronized because only the first thread does this.
         */
        private void writeHeader() throws IOException {
            // Write out the ConnectionHeader
            DataOutputBuffer buf = new DataOutputBuffer();
            header.write(buf);

            // Write out the payload length
            int bufLen = buf.getLength();
            out.writeInt(bufLen);
            out.write(buf.getData(), 0, bufLen);
        }

        /* wait till someone signals us to start reading RPC response or
         * it is idle too long, it is marked as to be closed,
         * or the client is marked as not running.
         *
         * Return true if it is time to read a response; false otherwise.
         */
        private synchronized boolean waitForWork() {
            if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
                long timeout = maxIdleTime-
                        (System.currentTimeMillis()-lastActivity.get());
                if (timeout>0) {
                    try {
                        wait(timeout);
                    } catch (InterruptedException e) {}
                }
            }

            if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
                return true;
            } else if (shouldCloseConnection.get()) {
                return false;
            } else if (calls.isEmpty()) { // idle connection closed or stopped
                markClosed(null);
                return false;
            } else { // get stopped but there are still pending requests
                markClosed((IOException)new IOException().initCause(
                        new InterruptedException()));
                return false;
            }
        }

        public InetSocketAddress getRemoteAddress() {
            return server;
        }

        /* Send a ping to the server if the time elapsed
         * since last I/O activity is equal to or greater than the ping interval
         */
        private synchronized void sendPing() throws IOException {
            long curTime = System.currentTimeMillis();
            if ( curTime - lastActivity.get() >= pingInterval) {
                lastActivity.set(curTime);
                synchronized (out) {
                    out.writeInt(PING_CALL_ID);
                    out.flush();
                }
            }
        }

        public void run() {
            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": starting, having connections "
                        + connections.size());

            while (waitForWork()) {//wait here for work - read or close connection
                receiveResponse();
            }

            close();

            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": stopped, remaining connections "
                        + connections.size());
        }

        /** Initiates a call by sending the parameter to the remote server.
         * Note: this is not called from the Connection thread, but by other
         * threads.
         */
        public void sendParam(Call call) {
            if (shouldCloseConnection.get()) {
                return;
            }

            DataOutputBuffer d=null;
            try {
                synchronized (this.out) {
                    if (LOG.isDebugEnabled())
                        LOG.debug(getName() + " sending #" + call.id);

                    //for serializing the
                    //data to be written
                    d = new DataOutputBuffer();
                    d.writeInt(call.id);
                    call.param.write(d);
                    byte[] data = d.getData();
                    int dataLength = d.getLength();
                    out.writeInt(dataLength);      //first put the data length
                    out.write(data, 0, dataLength);//write the data
                    out.flush();
                }
            } catch(IOException e) {
                markClosed(e);
            } finally {
                //the buffer is just an in-memory buffer, but it is still polite to
                // close early
                IOUtils.closeStream(d);
            }
        }

        /* Receive a response.
         * Because only one receiver, so no synchronization on in.
         */
        private void receiveResponse() {
            if (shouldCloseConnection.get()) {
                return;
            }
            touch();

            try {
                int id = in.readInt();                    // try to read an id

                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + " got value #" + id);

                Call call = calls.get(id);

                int state = in.readInt();     // read call status
                if (state == Status.SUCCESS.state) {
                    Writable value = ReflectionUtils.newInstance(valueClass);
                    value.readFields(in);                 // read value
                    call.setValue(value);
                    calls.remove(id);
                } else if (state == Status.ERROR.state) {
                    call.setException(new RemoteException(WritableUtils.readString(in),
                            WritableUtils.readString(in)));
                    calls.remove(id);
                } else if (state == Status.FATAL.state) {
                    // Close the connection
                    markClosed(new RemoteException(WritableUtils.readString(in),
                            WritableUtils.readString(in)));
                }
            } catch (IOException e) {
                markClosed(e);
            }
        }

        private synchronized void markClosed(IOException e) {
            if (shouldCloseConnection.compareAndSet(false, true)) {
                closeException = e;
                notifyAll();
            }
        }

        /** Close the connection. */
        private synchronized void close() {
            if (!shouldCloseConnection.get()) {
                LOG.error("The connection is not in the closed state");
                return;
            }

            // release the resources
            // first thing to do;take the connection out of the connection list
            synchronized (connections) {
                if (connections.get(remoteId) == this) {
                    connections.remove(remoteId);
                }
            }

            // close the streams and therefore the socket
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);

            // clean up all calls
            if (closeException == null) {
                if (!calls.isEmpty()) {
                    LOG.warn(
                            "A connection is closed for no cause and calls are not empty");

                    // clean up calls anyway
                    closeException = new IOException("Unexpected closed connection");
                    cleanupCalls();
                }
            } else {
                // log the info
                if (LOG.isDebugEnabled()) {
                    LOG.debug("closing ipc connection to " + server + ": " +
                            closeException.getMessage(),closeException);
                }

                // cleanup calls
                cleanupCalls();
            }
            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": closed");
        }

        /* Cleanup all calls and mark them as done */
        private void cleanupCalls() {
            Iterator<Map.Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
            while (itor.hasNext()) {
                Call c = itor.next().getValue();
                c.setException(closeException); // local exception
                itor.remove();
            }
        }
    }

    /** Call implementation used for parallel calls. */
    private class ParallelCall extends Call {
        private ParallelResults results;
        private int index;

        public ParallelCall(Writable param, ParallelResults results, int index) {
            super(param);
            this.results = results;
            this.index = index;
        }

        /** Deliver result to result collector. */
        protected void callComplete() {
            results.callComplete(this);
        }
    }

    /** Result collector for parallel calls. */
    private static class ParallelResults {
        private Writable[] values;
        private int size;
        private int count;

        public ParallelResults(int size) {
            this.values = new Writable[size];
            this.size = size;
        }

        /** Collect a result. */
        public synchronized void callComplete(ParallelCall call) {
            values[call.index] = call.value;            // store the value
            count++;                                    // count it
            if (count == size)                          // if all values are in
                notify();                                 // then notify waiting caller
        }
    }

    /** Construct an IPC client whose values are of the given {@link org.apache.hadoop.io.Writable}
     * class. */
    public Client(Class<? extends Writable> valueClass, SocketFactory factory) {
        this.valueClass = valueClass;
        this.socketFactory = factory;
    }

    /**
     * Construct an IPC client with the default SocketFactory
     * @param valueClass
     */
    public Client(Class<? extends Writable> valueClass) {
        this(valueClass, NetUtils.getSocketFactory());
    }

    /** Return the socket factory of this client
     *
     * @return this client's socket factory
     */
    SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /** Stop all threads related to this client.  No further calls may be made
     * using this client. */
    public void stop() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping client");
        }

        if (!running.compareAndSet(true, false)) {
            return;
        }

        // wake up all connections
        synchronized (connections) {
            for (Connection conn : connections.values()) {
                conn.interrupt();
            }
        }

        // wait until all connections are closed
        while (!connections.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    /** Make a call, passing <code>param</code>, to the IPC server running at
     * <code>address</code> which is servicing the <code>protocol</code> protocol,
     * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as timeout
     * and <code>conf</code> as configuration for this connection, returning the
     * value. Throws exceptions if there are network problems or if the remote code
     * threw an exception. */
    public Writable call(Writable param, InetSocketAddress addr,
                                              Class<?> protocol, int rpcTimeout)
            throws InterruptedException, IOException {
        ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol, rpcTimeout);
        return call(param, remoteId);
    }

    /** Make a call, passing <code>param</code>, to the IPC server defined by
     * <code>remoteId</code>, returning the value.
     * Throws exceptions if there are network problems or if the remote code
     * threw an exception. */
    public Writable call(Writable param, ConnectionId remoteId)
            throws InterruptedException, IOException {
        Call call = new Call(param);
        Connection connection = getConnection(remoteId, call);
        connection.sendParam(call);                 // send the parameter
        boolean interrupted = false;
        synchronized (call) {
            while (!call.done) {
                try {
                    call.wait();                           // wait for the result
                } catch (InterruptedException ie) {
                    // save the fact that we were interrupted
                    interrupted = true;
                }
            }

            if (interrupted) {
                // set the interrupt flag now that we are done waiting
                Thread.currentThread().interrupt();
            }

            if (call.error != null) {
                if (call.error instanceof RemoteException) {
                    call.error.fillInStackTrace();
                    throw call.error;
                } else { // local exception
                    // use the connection because it will reflect an ip change, unlike
                    // the remoteId
                    throw wrapException(connection.getRemoteAddress(), call.error);
                }
            } else {
                return call.value;
            }
        }
    }

    /**
     * Take an IOException and the address we were trying to connect to
     * and return an IOException with the input exception as the cause.
     * The new exception provides the stack trace of the place where
     * the exception is thrown and some extra diagnostics information.
     * If the exception is ConnectException or SocketTimeoutException,
     * return a new one of the same type; Otherwise return an IOException.
     *
     * @param addr target address
     * @param exception the relevant exception
     * @return an exception to throw
     */
    private IOException wrapException(InetSocketAddress addr,
                                      IOException exception) {
        if (exception instanceof ConnectException) {
            //connection refused; include the host:port in the error
            return (ConnectException)new ConnectException(
                    "Call to " + addr + " failed on connection exception: " + exception)
                    .initCause(exception);
        } else if (exception instanceof SocketTimeoutException) {
            return (SocketTimeoutException)new SocketTimeoutException(
                    "Call to " + addr + " failed on socket timeout exception: "
                            + exception).initCause(exception);
        } else {
            return (IOException)new IOException(
                    "Call to " + addr + " failed on local exception: " + exception)
                    .initCause(exception);

        }
    }


    /** Makes a set of calls in parallel.  Each parameter is sent to the
     * corresponding address.  When all values are available, or have timed out
     * or errored, the collected results are returned in an array.  The array
     * contains nulls for calls that timed out or errored.  */
    public Writable[] call(Writable[] params, InetSocketAddress[] addresses,
                                                Class<?> protocol)
            throws IOException, InterruptedException {
        if (addresses.length == 0) return new Writable[0];

        ParallelResults results = new ParallelResults(params.length);
        synchronized (results) {
            for (int i = 0; i < params.length; i++) {
                ParallelCall call = new ParallelCall(params[i], results, i);
                try {
                    ConnectionId remoteId = ConnectionId.getConnectionId(addresses[i],
                            protocol, 0);
                    Connection connection = getConnection(remoteId, call);
                    connection.sendParam(call);             // send each parameter
                } catch (IOException e) {
                    // log errors
                    LOG.info("Calling "+addresses[i]+" caught: " +
                            e.getMessage(),e);
                    results.size--;                         //  wait for one fewer result
                }
            }
            while (results.count != results.size) {
                try {
                    results.wait();                    // wait for all results
                } catch (InterruptedException e) {}
            }

            return results.values;
        }
    }

    //for unit testing only
    Set<ConnectionId> getConnectionIds() {
        synchronized (connections) {
            return connections.keySet();
        }
    }

    /** Get a connection from the pool, or create a new one and add it to the
     * pool.  Connections to a given ConnectionId are reused. */
    private Connection getConnection(ConnectionId remoteId,
                                     Call call)
            throws IOException, InterruptedException {
        if (!running.get()) {
            throw new IOException("The client is stopped");
        }
        Connection connection;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
        do {
            synchronized (connections) {
                connection = connections.get(remoteId);
                if (connection == null) {
                    connection = new Connection(remoteId);
                    connections.put(remoteId, connection);
                }
            }
        } while (!connection.addCall(call));

        //we don't invoke the method below inside "synchronized (connections)"
        //block above. The reason for that is if the server happens to be slow,
        //it will take longer to establish a connection and that will slow the
        //entire system down.
        connection.setupIOstreams();
        return connection;
    }

    /**
     * ����Э�����ֺ�Զ�˵�ַ��������ʶһ������
     */
    static class ConnectionId {
        InetSocketAddress address;
        Class<?> protocol;
        private static final int PRIME = 16777619;
        private int rpcTimeout;
        private String serverPrincipal;
        private int maxIdleTime; //connections will be culled if it was idle for
        //maxIdleTime msecs
        private final RetryPolicy connectionRetryPolicy;
        private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
        private int pingInterval; // how often sends ping to the server in msecs


        ConnectionId(InetSocketAddress address, Class<?> protocol, int rpcTimeout,
                     String serverPrincipal, int maxIdleTime,
                     RetryPolicy connectionRetryPolicy, boolean tcpNoDelay,
                     int pingInterval) {
            this.protocol = protocol;
            this.address = address;
            this.rpcTimeout = rpcTimeout;
            this.serverPrincipal = serverPrincipal;
            this.maxIdleTime = maxIdleTime;
            this.connectionRetryPolicy = connectionRetryPolicy;
            this.tcpNoDelay = tcpNoDelay;
            this.pingInterval = pingInterval;
        }

        InetSocketAddress getAddress() {
            return address;
        }

        Class<?> getProtocol() {
            return protocol;
        }

        private int getRpcTimeout() {
            return rpcTimeout;
        }

        String getServerPrincipal() {
            return serverPrincipal;
        }

        int getMaxIdleTime() {
            return maxIdleTime;
        }

        boolean getTcpNoDelay() {
            return tcpNoDelay;
        }

        int getPingInterval() {
            return pingInterval;
        }

        static ConnectionId getConnectionId(InetSocketAddress addr,
                                            Class<?> protocol) throws IOException {
            return getConnectionId(addr, protocol, 0);
        }

        static ConnectionId getConnectionId(InetSocketAddress addr,
                                            Class<?> protocol,  int rpcTimeout) throws IOException {
            return getConnectionId(addr, protocol, rpcTimeout, null);
        }

        static ConnectionId getConnectionId(InetSocketAddress addr,
                                            Class<?> protocol, int rpcTimeout,
                                            RetryPolicy connectionRetryPolicy) throws IOException {

            if (connectionRetryPolicy == null) {
                final int max = 3; //IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT

                connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        max, 1, TimeUnit.SECONDS);
            }

            String remotePrincipal = getRemotePrincipal(addr, protocol);
            return new ConnectionId(addr, protocol,
                    rpcTimeout, remotePrincipal,
                     10000, // 10s
                    connectionRetryPolicy,
                    false, //ipc.client.tcpnodelay,
                    Client.getPingInterval());
        }

        private static String getRemotePrincipal(InetSocketAddress address, Class<?> protocol) throws IOException {
            return null;
        }

        static boolean isEqual(Object a, Object b) {
            return a == null ? b == null : a.equals(b);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof ConnectionId) {
                ConnectionId that = (ConnectionId) obj;
                return isEqual(this.address, that.address)
                        && this.maxIdleTime == that.maxIdleTime
                        && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
                        && this.pingInterval == that.pingInterval
                        && isEqual(this.protocol, that.protocol)
                        && this.rpcTimeout == that.rpcTimeout
                        && isEqual(this.serverPrincipal, that.serverPrincipal)
                        && this.tcpNoDelay == that.tcpNoDelay;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = connectionRetryPolicy.hashCode();
            result = PRIME * result + ((address == null) ? 0 : address.hashCode());
            result = PRIME * result + maxIdleTime;
            result = PRIME * result + pingInterval;
            result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
            result = PRIME * rpcTimeout;
            result = PRIME * result
                    + ((serverPrincipal == null) ? 0 : serverPrincipal.hashCode());
            result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
            return result;
        }
    }
}
