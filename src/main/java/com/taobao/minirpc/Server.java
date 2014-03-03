package com.taobao.minirpc;

import com.taobao.utils.Configuration;
import com.taobao.utils.StringUtils;
import com.taobao.utils.WritableUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by shili on 14-2-27.
 */
public abstract class Server {
    private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();

    public void addTerseExceptions(Class<?>... exceptionClass) {
        exceptionsHandler.addTerseExceptions(exceptionClass);
    }

    /**
     * ExceptionsHandler manages Exception groups for special handling e.g., terse
     * exception group for concise logging messages
     */
    static class ExceptionsHandler {
        private volatile Set<String> terseExceptions = new HashSet<String>();

        /**
         * Add exception class so server won't log its stack trace. Modifying the
         * terseException through this method is thread safe.
         *
         * @param exceptionClass
         *          exception classes
         */
        void addTerseExceptions(Class<?>... exceptionClass) {

            // Make a copy of terseException for performing modification
            final HashSet<String> newSet = new HashSet<String>(terseExceptions);

            // Add all class names into the HashSet
            for (Class<?> name : exceptionClass) {
                newSet.add(name.toString());
            }
            // Replace terseException set
            terseExceptions = Collections.unmodifiableSet(newSet);
        }

        boolean isTerse(Class<?> t) {
            return terseExceptions.contains(t.toString());
        }
    }

    /**
     * The first four bytes of Hadoop RPC connections
     */
    public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());

    // 1 : Introduce ping and server does not throw away RPCs
    // 3 : Introduce the protocol into the RPC connection header
    // 4 : Introduced SASL security layer
    public static final byte CURRENT_VERSION = 4;

    /**
     * How many calls/handler are allowed in the queue.
     */
    private static final int IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;
    private static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY =
            "ipc.server.handler.queue.size";

    /**
     * Initial and max size of response buffer
     */
    static int INITIAL_RESP_BUF_SIZE = 10240;
    static final String IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY =
            "ipc.server.max.response.size";
    static final int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024*1024;

    public static final Log LOG = LogFactory.getLog(Server.class);
    private static final Log AUDITLOG =
            LogFactory.getLog("SecurityLogger."+Server.class.getName());
    private static final String AUTH_FAILED_FOR = "Auth failed for ";
    private static final String AUTH_SUCCESSFULL_FOR = "Auth successfull for ";

    private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

    private static final Map<String, Class<?>> PROTOCOL_CACHE =
            new ConcurrentHashMap<String, Class<?>>();

    static Class<?> getProtocolClass(String protocolName, Configuration conf)
            throws ClassNotFoundException {
        Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
        if (protocol == null) {
            protocol = Class.forName(protocolName);
            PROTOCOL_CACHE.put(protocolName, protocol);
        }
        return protocol;
    }

    public static Server get() {
        return SERVER.get();
    }

    /** This is set to Call object before Handler invokes an RPC and reset
     * after the call returns.
     */
    private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

    /** Returns the remote side ip address when invoked inside an RPC
     *  Returns null incase of an error.
     */
    public static InetAddress getRemoteIp() {
        Call call = CurCall.get();
        if (call != null) {
            return call.connection.socket.getInetAddress();
        }
        return null;
    }
    /** Returns remote address as a string when invoked inside an RPC.
     *  Returns null in case of an error.
     */
    public static String getRemoteAddress() {
        InetAddress addr = getRemoteIp();
        return (addr == null) ? null : addr.getHostAddress();
    }

    private String bindAddress;
    private int port;                               // port we listen on
    private int handlerCount;                       // number of handler threads
    private int readThreads;                        // number of read threads
    private Class<? extends Writable> paramClass;   // class of call parameters
    private int maxIdleTime;                        // the maximum idle time after
    // which a client may be disconnected
    private int thresholdIdleConnections;           // the number of idle connections
    // after which we will start
    // cleaning up idle
    // connections
    int maxConnectionsToNuke;                       // the max number of
    // connections to nuke
    //during a cleanup

    private Configuration conf;
    private int maxQueueSize;
    private final int maxRespSize;
    private int socketSendBufferSize;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

    volatile private boolean running = true;         // true while server runs
    private BlockingQueue<Call> callQueue; // queued calls

    private List<Connection> connectionList =
            Collections.synchronizedList(new LinkedList<Connection>());
    //maintain a list
    //of client connections
    private Listener listener = null;
    private Responder responder = null;
    private int numConnections = 0;
    private Handler[] handlers = null;


    public static void bind(ServerSocket socket, InetSocketAddress address,
                            int backlog) throws IOException {
        try {
            socket.bind(address, backlog);
        } catch (BindException e) {
            BindException bindException = new BindException("Problem binding to " + address
                    + " : " + e.getMessage());
            bindException.initCause(e);
            throw bindException;
        } catch (SocketException e) {
            // If they try to bind to a different host's address, give a better
            // error message.
            if ("Unresolved address".equals(e.getMessage())) {
                throw new UnknownHostException("Invalid hostname for server: " +
                        address.getHostName());
            } else {
                throw e;
            }
        }
    }

    /** A call queued for handling. */
    private static class Call {
        private int id;                               // the client's call id
        private Writable param;                       // the parameter passed
        private Connection connection;                // connection to client
        private long timestamp;     // the time received when response is null
        // the time served when response is not null
        private ByteBuffer response;                      // the response for this call

        public Call(int id, Writable param, Connection connection) {
            this.id = id;
            this.param = param;
            this.connection = connection;
            this.timestamp = System.currentTimeMillis();
            this.response = null;
        }

        @Override
        public String toString() {
            return param.toString() + " from " + connection.toString();
        }

        public void setResponse(ByteBuffer response) {
            this.response = response;
        }
    }

    /** Listens on the socket. Creates jobs for the handler threads*/
    private class Listener extends Thread {

        private ServerSocketChannel acceptChannel = null; //the accept channel
        private Selector selector = null; //the selector that we use for the server
        private Reader[] readers = null;
        private int currentReader = 0;
        private InetSocketAddress address; //the address we bind at
        private Random rand = new Random();
        private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
        //-tion (for idle connections) ran
        private long cleanupInterval = 10000; //the minimum interval between
        //two cleanup runs
        private int backlogLength = 128;
        private ExecutorService readPool;

        public Listener() throws IOException {
            address = new InetSocketAddress(bindAddress, port);
            // Create a new server socket and set to non blocking mode
            acceptChannel = ServerSocketChannel.open();
            acceptChannel.configureBlocking(false);

            // Bind the server socket to the local host and port
            bind(acceptChannel.socket(), address, backlogLength);
            port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
            // create a selector;
            selector= Selector.open();
            readers = new Reader[readThreads];
            readPool = Executors.newFixedThreadPool(readThreads);
            for (int i = 0; i < readThreads; i++) {
                Selector readSelector = Selector.open();
                Reader reader = new Reader(readSelector);
                readers[i] = reader;
                readPool.execute(reader);
            }

            // Register accepts on the server socket with the selector.
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
            this.setName("IPC Server listener on " + port);
            this.setDaemon(true);
        }

        private class Reader implements Runnable {
            private volatile boolean adding = false;
            private Selector readSelector = null;

            Reader(Selector readSelector) {
                this.readSelector = readSelector;
            }
            public void run() {
                LOG.info("Starting SocketReader");
                synchronized (this) {
                    while (running) {
                        SelectionKey key = null;
                        try {
                            readSelector.select();
                            while (adding) {
                                this.wait(1000);
                            }

                            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                            while (iter.hasNext()) {
                                key = iter.next();
                                iter.remove();
                                if (key.isValid()) {
                                    if (key.isReadable()) {
                                        doRead(key);
                                    }
                                }
                                key = null;
                            }
                        } catch (InterruptedException e) {
                            if (running) {                      // unexpected -- log it
                                LOG.info(getName() + " caught: " +
                                        StringUtils.stringifyException(e));
                            }
                        } catch (IOException ex) {
                            LOG.error("Error in Reader", ex);
                        }
                    }
                }
            }

            /**
             * This gets reader into the state that waits for the new channel
             * to be registered with readSelector. If it was waiting in select()
             * the thread will be woken up, otherwise whenever select() is called
             * it will return even if there is nothing to read and wait
             * in while(adding) for finishAdd call
             */
            public void startAdd() {
                adding = true;
                readSelector.wakeup();
            }

            public synchronized SelectionKey registerChannel(SocketChannel channel)
                    throws IOException {
                return channel.register(readSelector, SelectionKey.OP_READ);
            }

            public synchronized void finishAdd() {
                adding = false;
                this.notify();
            }
        }

        /** cleanup connections from connectionList. Choose a random range
         * to scan and also have a limit on the number of the connections
         * that will be cleanedup per run. The criteria for cleanup is the time
         * for which the connection was idle. If 'force' is true then all
         * connections will be looked at for the cleanup.
         */
        private void cleanupConnections(boolean force) {
            if (force || numConnections > thresholdIdleConnections) {
                long currentTime = System.currentTimeMillis();
                if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
                    return;
                }
                int start = 0;
                int end = numConnections - 1;
                if (!force) {
                    start = rand.nextInt() % numConnections;
                    end = rand.nextInt() % numConnections;
                    int temp;
                    if (end < start) {
                        temp = start;
                        start = end;
                        end = temp;
                    }
                }
                int i = start;
                int numNuked = 0;
                while (i <= end) {
                    Connection c;
                    synchronized (connectionList) {
                        try {
                            c = connectionList.get(i);
                        } catch (Exception e) {return;}
                    }
                    if (c.timedOut(currentTime)) {
                        if (LOG.isDebugEnabled())
                            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
                        closeConnection(c);
                        numNuked++;
                        end--;
                        c = null;
                        if (!force && numNuked == maxConnectionsToNuke) break;
                    }
                    else i++;
                }
                lastCleanupRunTime = System.currentTimeMillis();
            }
        }

        @Override
        public void run() {
            LOG.info(getName() + ": starting");
            SERVER.set(Server.this);
            while (running) {
                SelectionKey key = null;
                try {
                    selector.select();
                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid()) {
                                if (key.isAcceptable())
                                    doAccept(key);
                            }
                        } catch (IOException e) {
                        }
                        key = null;
                    }
                } catch (OutOfMemoryError e) {
                    // we can run out of memory if we have too many threads
                    // log the event and sleep for a minute and give
                    // some thread(s) a chance to finish
                    LOG.warn("Out of Memory in server select", e);
                    closeCurrentConnection(key, e);
                    cleanupConnections(true);
                    try { Thread.sleep(60000); } catch (Exception ie) {}
                } catch (Exception e) {
                    closeCurrentConnection(key, e);
                }
                cleanupConnections(false);
            }
            LOG.info("Stopping " + this.getName());

            synchronized (this) {
                try {
                    acceptChannel.close();
                    selector.close();
                } catch (IOException e) { }

                selector= null;
                acceptChannel= null;

                // clean up all connections
                while (!connectionList.isEmpty()) {
                    closeConnection(connectionList.remove(0));
                }
            }
        }

        private void closeCurrentConnection(SelectionKey key, Throwable e) {
            if (key != null) {
                Connection c = (Connection)key.attachment();
                if (c != null) {
                    if (LOG.isDebugEnabled())
                        LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
                    closeConnection(c);
                    c = null;
                }
            }
        }

        InetSocketAddress getAddress() {
            return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
        }

        void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
            Connection c = null;
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            while ((channel = server.accept()) != null) {
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(tcpNoDelay);
                Reader reader = getReader();
                try {
                    reader.startAdd();
                    SelectionKey readKey = reader.registerChannel(channel);
                    c = new Connection(readKey, channel, System.currentTimeMillis());
                    readKey.attach(c);
                    synchronized (connectionList) {
                        connectionList.add(numConnections, c);
                        numConnections++;
                    }
                    if (LOG.isDebugEnabled())
                        LOG.debug("Server connection from " + c.toString() +
                                "; # active connections: " + numConnections +
                                "; # queued calls: " + callQueue.size());
                } finally {
                    reader.finishAdd();
                }

            }
        }

        void doRead(SelectionKey key) throws InterruptedException {
            LOG.info("Reader begin to read");
            int count = 0;
            Connection c = (Connection)key.attachment();
            if (c == null) {
                return;
            }
            c.setLastContact(System.currentTimeMillis());

            try {
                count = c.readAndProcess();
            } catch (InterruptedException ieo) {
                LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
                throw ieo;
            } catch (Exception e) {
                LOG.info(getName() + ": readAndProcess threw exception " + e + ". Count of bytes read: " + count, e);
                count = -1; //so that the (count < 0) block is executed
            }
            if (count < 0) {
                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + ": disconnecting client " +
                            c + ". Number of active connections: "+
                            numConnections);
                closeConnection(c);
                c = null;
            }
            else {
                c.setLastContact(System.currentTimeMillis());
            }
        }

        synchronized void doStop() {
            if (selector != null) {
                selector.wakeup();
                Thread.yield();
            }
            if (acceptChannel != null) {
                try {
                    acceptChannel.socket().close();
                } catch (IOException e) {
                    LOG.info(getName() + ":Exception in closing listener socket. " + e);
                }
            }
            readPool.shutdown();
        }

        // The method that will return the next reader to work with
        // Simplistic implementation of round robin for now
        Reader getReader() {
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }

    }

    // Sends responses of RPC back to clients.
    private class Responder extends Thread {
        private Selector writeSelector;
        private int pending;         // connections waiting to register

        final static int PURGE_INTERVAL = 900000; // 15mins

        Responder() throws IOException {
            this.setName("IPC Server Responder");
            this.setDaemon(true);
            writeSelector = Selector.open(); // create a selector
            pending = 0;
        }

        @Override
        public void run() {
            LOG.info(getName() + ": starting");
            SERVER.set(Server.this);
            long lastPurgeTime = 0;   // last check for old calls.

            while (running) {
                try {
                    waitPending();     // If a channel is being registered, wait.
                    writeSelector.select(PURGE_INTERVAL);
                    Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid() && key.isWritable()) {
                                doAsyncWrite(key);
                            }
                        } catch (IOException e) {
                            LOG.info(getName() + ": doAsyncWrite threw exception " + e);
                        }
                    }
                    long now = System.currentTimeMillis();
                    if (now < lastPurgeTime + PURGE_INTERVAL) {
                        continue;
                    }
                    lastPurgeTime = now;
                    //
                    // If there were some calls that have not been sent out for a
                    // long time, discard them.
                    //
                    LOG.debug("Checking for old call responses.");
                    ArrayList<Call> calls;

                    // get the list of channels from list of keys.
                    synchronized (writeSelector.keys()) {
                        calls = new ArrayList<Call>(writeSelector.keys().size());
                        iter = writeSelector.keys().iterator();
                        while (iter.hasNext()) {
                            SelectionKey key = iter.next();
                            Call call = (Call)key.attachment();
                            if (call != null && key.channel() == call.connection.channel) {
                                calls.add(call);
                            }
                        }
                    }

                    for(Call call : calls) {
                        try {
                            doPurge(call, now);
                        } catch (IOException e) {
                            LOG.warn("Error in purging old calls " + e);
                        }
                    }
                } catch (OutOfMemoryError e) {
                    //
                    // we can run out of memory if we have too many threads
                    // log the event and sleep for a minute and give
                    // some thread(s) a chance to finish
                    //
                    LOG.warn("Out of Memory in server select", e);
                    try { Thread.sleep(60000); } catch (Exception ie) {}
                } catch (Exception e) {
                    LOG.warn("Exception in Responder " +
                            StringUtils.stringifyException(e));
                }
            }
            LOG.info("Stopping " + this.getName());
        }

        private void doAsyncWrite(SelectionKey key) throws IOException {
            Call call = (Call)key.attachment();
            if (call == null) {
                return;
            }
            if (key.channel() != call.connection.channel) {
                throw new IOException("doAsyncWrite: bad channel");
            }

            synchronized(call.connection.responseQueue) {
                if (processResponse(call.connection.responseQueue, false)) {
                    try {
                        key.interestOps(0);
                    } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
                        LOG.warn("Exception while changing ops : " + e);
                    }
                }
            }
        }

        //
        // Remove calls that have been pending in the responseQueue
        // for a long time.
        //
        private void doPurge(Call call, long now) throws IOException {
            LinkedList<Call> responseQueue = call.connection.responseQueue;
            synchronized (responseQueue) {
                Iterator<Call> iter = responseQueue.listIterator(0);
                while (iter.hasNext()) {
                    call = iter.next();
                    if (now > call.timestamp + PURGE_INTERVAL) {
                        closeConnection(call.connection);
                        break;
                    }
                }
            }
        }

        // Processes one response. Returns true if there are no more pending
        // data for this channel.
        //
        private boolean processResponse(LinkedList<Call> responseQueue,
                                        boolean inHandler) throws IOException {
            boolean error = true;
            boolean done = false;       // there is more data for this channel.
            int numElements = 0;
            Call call = null;
            try {
                synchronized (responseQueue) {
                    //
                    // If there are no items for this channel, then we are done
                    //
                    numElements = responseQueue.size();
                    if (numElements == 0) {
                        error = false;
                        return true;              // no more data for this channel.
                    }
                    //
                    // Extract the first call
                    //
                    call = responseQueue.removeFirst();
                    SocketChannel channel = call.connection.channel;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getName() + ": responding to #" + call.id + " from " +
                                call.connection);
                    }
                    //
                    // Send as much data as we can in the non-blocking fashion
                    //
                    int numBytes = channelWrite(channel, call.response);
                    if (numBytes < 0) {
                        return true;
                    }
                    if (!call.response.hasRemaining()) {
                        call.connection.decRpcCount();
                        if (numElements == 1) {    // last call fully processes.
                            done = true;             // no more data for this channel.
                        } else {
                            done = false;            // more calls pending to be sent.
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                                    call.connection + " Wrote " + numBytes + " bytes.");
                        }
                    } else {
                        //
                        // If we were unable to write the entire response out, then
                        // insert in Selector queue.
                        //
                        call.connection.responseQueue.addFirst(call);

                        if (inHandler) {
                            // set the serve time when the response has to be sent later
                            call.timestamp = System.currentTimeMillis();

                            incPending();
                            try {
                                // Wakeup the thread blocked on select, only then can the call
                                // to channel.register() complete.
                                writeSelector.wakeup();
                                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
                            } catch (ClosedChannelException e) {
                                //Its ok. channel might be closed else where.
                                done = true;
                            } finally {
                                decPending();
                            }
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                                    call.connection + " Wrote partial " + numBytes +
                                    " bytes.");
                        }
                    }
                    error = false;              // everything went off well
                }
            } finally {
                if (error && call != null) {
                    LOG.warn(getName()+", call " + call + ": output error");
                    done = true;               // error. no more data for this channel.
                    closeConnection(call.connection);
                }
            }
            return done;
        }

        //
        // Enqueue a response from the application.
        //
        void doRespond(Call call) throws IOException {
            synchronized (call.connection.responseQueue) {
                call.connection.responseQueue.addLast(call);
                if (call.connection.responseQueue.size() == 1) {
                    processResponse(call.connection.responseQueue, true);
                }
            }
        }

        private synchronized void incPending() {   // call waiting to be enqueued.
            pending++;
        }

        private synchronized void decPending() { // call done enqueueing.
            pending--;
            notify();
        }

        private synchronized void waitPending() throws InterruptedException {
            while (pending > 0) {
                wait();
            }
        }
    }

    /** Reads calls from a connection and queues them for handling. */
    public class Connection {
        private boolean rpcHeaderRead = false; // if initial rpc header is read
        private boolean headerRead = false;  //if the connection header that
        //follows version is read.

        private SocketChannel channel;
        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;
        private LinkedList<Call> responseQueue;
        private volatile int rpcCount = 0; // number of outstanding rpcs
        private long lastContact;
        private int dataLength;
        private Socket socket;
        // Cache the remote host & port info so that even if the socket is
        // disconnected, we can say where it used to connect to.
        private String hostAddress;
        private int remotePort;
        private InetAddress addr;

        ConnectionHeader header = new ConnectionHeader();
        Class<?> protocol;
        private ByteBuffer rpcHeaderBuffer;

        // Fake 'call' for failed authorization response
        private final int AUTHROIZATION_FAILED_CALLID = -1;
        private final Call authFailedCall =
                new Call(AUTHROIZATION_FAILED_CALLID, null, this);
        private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();

        private boolean useWrap = false;

        public Connection(SelectionKey key, SocketChannel channel,
                          long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.data = null;
            this.dataLengthBuffer = ByteBuffer.allocate(4);
            this.socket = channel.socket();
            this.addr = socket.getInetAddress();
            if (addr == null) {
                this.hostAddress = "*Unknown*";
            } else {
                this.hostAddress = addr.getHostAddress();
            }
            this.remotePort = socket.getPort();
            this.responseQueue = new LinkedList<Call>();
            if (socketSendBufferSize != 0) {
                try {
                    socket.setSendBufferSize(socketSendBufferSize);
                } catch (IOException e) {
                    LOG.warn("Connection: unable to set socket send buffer size to " +
                            socketSendBufferSize);
                }
            }
        }

        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }

        public String getHostAddress() {
            return hostAddress;
        }

        public InetAddress getHostInetAddress() {
            return addr;
        }

        public void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }

        public long getLastContact() {
            return lastContact;
        }

        /* Return true if the connection has no outstanding rpc */
        private boolean isIdle() {
            return rpcCount == 0;
        }

        /* Decrement the outstanding RPC count */
        private void decRpcCount() {
            rpcCount--;
        }

        /* Increment the outstanding RPC count */
        private void incRpcCount() {
            rpcCount++;
        }

        private boolean timedOut(long currentTime) {
            if (isIdle() && currentTime -  lastContact > maxIdleTime)
                return true;
            return false;
        }

        public int readAndProcess() throws IOException, InterruptedException {
            while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */
                int count = -1;
                if (dataLengthBuffer.remaining() > 0) {
                    count = channelRead(channel, dataLengthBuffer);
                    if (count < 0 || dataLengthBuffer.remaining() > 0)
                        return count;
                }

                if (!rpcHeaderRead) {
                    //Every connection is expected to send the header.
                    if (rpcHeaderBuffer == null) {
                        rpcHeaderBuffer = ByteBuffer.allocate(1);
                    }
                    count = channelRead(channel, rpcHeaderBuffer);
                    if (count < 0 || rpcHeaderBuffer.remaining() > 0) {
                        return count;
                    }
                    int version = rpcHeaderBuffer.get(0);
                    dataLengthBuffer.flip();
                    if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) {
                        //Warning is ok since this is not supposed to happen.
                        LOG.warn("Incorrect header or version mismatch from " +
                                hostAddress + ":" + remotePort +
                                " got version " + version +
                                " expected version " + CURRENT_VERSION);
                        return -1;
                    }
                    dataLengthBuffer.clear();

                    rpcHeaderBuffer = null;
                    rpcHeaderRead = true;
                    continue;
                }

                if (data == null) {
                    dataLengthBuffer.flip();
                    dataLength = dataLengthBuffer.getInt();

                    if (dataLength == Client.PING_CALL_ID) {
                        if(!useWrap) { //covers the !useSasl too
                            dataLengthBuffer.clear();
                            return 0;  //ping message
                        }
                    }
                    if (dataLength < 0) {
                        LOG.warn("Unexpected data length " + dataLength + "!! from " +
                                getHostAddress());
                    }
                    data = ByteBuffer.allocate(dataLength);
                }

                count = channelRead(channel, data);

                if (data.remaining() == 0) {
                    dataLengthBuffer.clear();
                    data.flip();
                    boolean isHeaderRead = headerRead;
                    /**
                     * 处理rpc数据逻辑
                     */
                    processOneRpc(data.array());

                    data = null;
                    if (!isHeaderRead) {
                        continue;
                    }
                }
                return count;
            }
        }

        /// Reads the connection header following version
        private void processHeader(byte[] buf) throws IOException {
            DataInputStream in =
                    new DataInputStream(new ByteArrayInputStream(buf));
            header.readFields(in);
            try {
                String protocolClassName = header.getProtocol();
                if (protocolClassName != null) {
                    protocol = getProtocolClass(header.getProtocol(), conf);
                }
            } catch (ClassNotFoundException cnfe) {
                throw new IOException("Unknown protocol: " + header.getProtocol());
            }
        }

        private void processOneRpc(byte[] buf) throws IOException,
                InterruptedException {
            if (headerRead) {
                LOG.info("Connection start to process Data");
                processData(buf);
            } else {
                LOG.info("Connection start to process header");
                processHeader(buf);
                headerRead = true;
            }
        }

        private void processData(byte[] buf) throws  IOException, InterruptedException {
            DataInputStream dis =
                    new DataInputStream(new ByteArrayInputStream(buf));
            int id = dis.readInt();                    // try to read an id

            if (LOG.isDebugEnabled())
                LOG.debug(" got #" + id);

            //Writable param = ReflectionUtils.newInstance(paramClass);//read param
            Writable param = null;
            /**
             * add the reflect directly
             */
            try {
                Constructor meth = paramClass.getDeclaredConstructor(new Class[]{});
                meth.setAccessible(true);
                param = (Writable)meth.newInstance();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            if (param != null) {
                param.readFields(dis);
            }

            Call call = new Call(id, param, this);
            callQueue.put(call);              // queue the call; maybe blocked here
            incRpcCount();  // Increment the rpc count
        }

        private synchronized void close() throws IOException {
            data = null;
            dataLengthBuffer = null;
            if (!channel.isOpen())
                return;
            try {socket.shutdownOutput();} catch(Exception e) {}
            if (channel.isOpen()) {
                try {channel.close();} catch(Exception e) {}
            }
            try {socket.close();} catch(Exception e) {}
        }
    }

    /** Handles queued calls . */
    private class Handler extends Thread {
        public Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("IPC Server handler "+ instanceNumber + " on " + port);
        }

        @Override
        public void run() {
            LOG.info(getName() + ": starting");
            SERVER.set(Server.this);
            ByteArrayOutputStream buf =
                    new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
            while (running) {
                try {
                    final Call call = callQueue.take(); // pop the queue; maybe blocked here
                    LOG.info(getName() + ": has #" + call.id + " from " + call.connection);

                    if (LOG.isDebugEnabled())
                        LOG.debug(getName() + ": has #" + call.id + " from " +
                                call.connection);

                    String errorClass = null;
                    String error = null;
                    Writable value = null;

                    CurCall.set(call);
                    try {
                        // 调用抽象方法call直接去请求，去掉用户验证的步骤
                        value = call(call.connection.protocol, call.param, call.timestamp);
                    } catch (Throwable e) {
                        String logMsg = getName() + ", call " + call + ": error: " + e;
                        if (e instanceof RuntimeException || e instanceof Error) {
                            LOG.warn(logMsg, e);
                        } else if (exceptionsHandler.isTerse(e.getClass())) {
                            LOG.info(logMsg);
                        } else {
                            LOG.info(logMsg, e);
                        }
                        errorClass = e.getClass().getName();
                        error = StringUtils.stringifyException(e);
                    }
                    CurCall.set(null);
                    synchronized (call.connection.responseQueue) {
                        // setupResponse() needs to be sync'ed together with
                        // responder.doResponse() since setupResponse may use
                        // SASL to encrypt response data and SASL enforces
                        // its own message ordering.
                        setupResponse(buf, call,
                                (error == null) ? Status.SUCCESS : Status.ERROR,
                                value, errorClass, error);
                        // Discard the large buf and reset it back to
                        // smaller size to freeup heap
                        if (buf.size() > maxRespSize) {
                            LOG.warn("Large response size " + buf.size() + " for call " +
                                    call.toString());
                            buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
                        }
                        responder.doRespond(call);
                    }
                } catch (InterruptedException e) {
                    if (running) {                          // unexpected -- log it
                        LOG.info(getName() + " caught: " +
                                StringUtils.stringifyException(e));
                    }
                } catch (Exception e) {
                    LOG.info(getName() + " caught: " +
                            StringUtils.stringifyException(e));
                }
            }
            LOG.info(getName() + ": exiting");
        }

    }

    /** Constructs a server listening on the named port and address.  Parameters passed must
     * be of the named class.  The <code>handlerCount</handlerCount> determines
     * the number of handler threads that will be used to process calls.
     *
     */
    @SuppressWarnings("unchecked")
    protected Server(String bindAddress, int port,
                     Class<? extends Writable> paramClass, int handlerCount, String serverName)
            throws IOException {
        this.bindAddress = bindAddress;
        this.port = port;
        this.paramClass = paramClass;
        this.handlerCount = handlerCount;
        this.socketSendBufferSize = 0;
        this.maxQueueSize = handlerCount * 10;  //IPC_SERVER_HANDLER_QUEUE_SIZE_KEY, IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT
        this.maxRespSize = 1000;      //IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT
        this.readThreads = 10;   //IPC_SERVER_RPC_READ_THREADS_KEY, IPC_SERVER_RPC_READ_THREADS_DEFAULT
        this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize);
        this.maxIdleTime = 2*1000;  //ipc.client.connection.maxidletime
        this.maxConnectionsToNuke = 10;
        this.thresholdIdleConnections = 4000;

        // Start the listener here and let it bind to the port
        listener = new Listener();
        this.port = listener.getAddress().getPort();
        this.tcpNoDelay = false;

        // Create the responder here
        responder = new Responder();
    }

    private void closeConnection(Connection connection) {
        synchronized (connectionList) {
            if (connectionList.remove(connection))
                numConnections--;
        }
        try {
            connection.close();
        } catch (IOException e) {
        }
    }

    /**
     * Setup response for the IPC Call.
     *
     * @param response buffer to serialize the response into
     * @param call {@link Call} to which we are setting up the response
     * @param status {@link Status} of the IPC call
     * @param rv return value for the IPC Call, if the call was successful
     * @param errorClass error class, if the the call failed
     * @param error error message, if the call failed
     * @throws IOException
     */
    private void setupResponse(ByteArrayOutputStream response,
                               Call call, Status status,
                               Writable rv, String errorClass, String error)
            throws IOException {
        response.reset();
        DataOutputStream out = new DataOutputStream(response);
        out.writeInt(call.id);                // write call id
        out.writeInt(status.state);           // write status

        if (status == Status.SUCCESS) {
            rv.write(out);
        } else {
            WritableUtils.writeString(out, errorClass);
            WritableUtils.writeString(out, error);
        }
        call.setResponse(ByteBuffer.wrap(response.toByteArray()));
    }

    /** Sets the socket buffer size used for responding to RPCs */
    public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

    /** Server 启动函数，启动Listener监听线程, Responder回复线程, 多个Handler调用线程 */
    public synchronized void start() {
        responder.start();
        listener.start();
        handlers = new Handler[handlerCount];

        for (int i = 0; i < handlerCount; i++) {
            handlers[i] = new Handler(i);
            handlers[i].start();
        }
    }

    /** Stops the service.  No new calls will be handled after this is called. */
    public synchronized void stop() {
        LOG.info("Stopping server on " + port);
        running = false;
        if (handlers != null) {
            for (int i = 0; i < handlerCount; i++) {
                if (handlers[i] != null) {
                    handlers[i].interrupt();
                }
            }
        }
        listener.interrupt();
        listener.doStop();
        responder.interrupt();
        notifyAll();
    }

    /** Wait for the server to be stopped.
     * Does not wait for all subthreads to finish.
     *  See {@link #stop()}.
     */
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    /**
     * Return the socket (ip+port) on which the RPC server is listening to.
     * @return the socket (ip+port) on which the RPC server is listening to.
     */
    public synchronized InetSocketAddress getListenerAddress() {
        return listener.getAddress();
    }

    /** Called for each call. */
    public abstract Writable call(Class<?> protocol,
                                  Writable param, long receiveTime)
            throws IOException;


    /**
     * The number of open RPC conections
     * @return the number of open rpc connections
     */
    public int getNumOpenConnections() {
        return numConnections;
    }

    /**
     * The number of rpc calls in the queue.
     * @return The number of rpc calls in the queue.
     */
    public int getCallQueueLen() {
        return callQueue.size();
    }


    /**
     * When the read or write buffer size is larger than this limit, i/o will be
     * done in chunks of this size. Most RPC requests and responses would be
     * be smaller.
     */
    private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.

    /**
     * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
     * If the amount of data is large, it writes to channel in smaller chunks.
     * This is to avoid jdk from creating many direct buffers as the size of
     * buffer increases. This also minimizes extra copies in NIO layer
     * as a result of multiple write operations required to write a large
     * buffer.
     *
     * @see WritableByteChannel#write(ByteBuffer)
     */
    private int channelWrite(WritableByteChannel channel,
                             ByteBuffer buffer) throws IOException {

        int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.write(buffer) : channelIO(null, channel, buffer);
        return count;
    }


    /**
     * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
     * If the amount of data is large, it writes to channel in smaller chunks.
     * This is to avoid jdk from creating many direct buffers as the size of
     * ByteBuffer increases. There should not be any performance degredation.
     *
     * @see ReadableByteChannel#read(ByteBuffer)
     */
    private int channelRead(ReadableByteChannel channel,
                            ByteBuffer buffer) throws IOException {

        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    /**
     * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
     * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
     * one of readCh or writeCh should be non-null.
     *
     * @see #channelRead(ReadableByteChannel, ByteBuffer)
     * @see #channelWrite(WritableByteChannel, ByteBuffer)
     */
    private static int channelIO(ReadableByteChannel readCh,
                                 WritableByteChannel writeCh,
                                 ByteBuffer buf) throws IOException {

        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;

        while (buf.remaining() > 0) {
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize) {
                    break;
                }

            } finally {
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
    }
}
