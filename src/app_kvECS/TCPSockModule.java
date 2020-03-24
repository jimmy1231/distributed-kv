package app_kvECS;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.UnifiedMessage;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class TCPSockModule {
    private static Logger logger = LoggerFactory.getLogger(TCPSockModule.class);
    private static int MAX_READ_BYTES = 4096;
    private static String DEADBEEF = "_______DEADBEEF_______";
    private static Map<String, Socket> CONNECTION_POOL = new HashMap<>();

    private InputStream input;
    private OutputStream output;
    private Socket socket;
    private String host;
    private int port;

    /****************************************************/
    public TCPSockModule(String host, int port) throws Exception {
        /* Establish socket connection */
        this.host = host;
        this.port = port;
        socket = connect(host, port, -1);
        logger.debug(String.format(
            "ECSSocket connection established: %s:%d",
            host, port));

        input = this.socket.getInputStream();
        output = this.socket.getOutputStream();
    }

    public TCPSockModule(String host, int port, int timeout) throws Exception {
        /* Establish socket connection with timeout */
        this.host = host;
        this.port = port;
        socket = connect(host, port, timeout);
        logger.trace(String.format(
            "ECSSocket connection established: %s:%d",
            host, port));

        input = this.socket.getInputStream();
        output = this.socket.getOutputStream();
    }
    /****************************************************/

    /**
     * Do request. Blocks until response is received.
     *
     * @param request
     */
    public UnifiedMessage doRequest(UnifiedMessage request) throws Exception {
        UnifiedMessage resp;

        /* Do request */
        String rheader = StringUtils.repeat("-",35);
        logger.debug(rheader);
        logger.debug("REQUEST -> {}:{} MessageType={}, StatusType={}",
            socket.getLocalAddress(), socket.getPort(),
            request.getMessageType(),
            request.getStatusType());
        if (!send(output, request.serialize())) {
            logger.debug("Failed to send request");
            throw new Exception("SEND failed");
        }

        /* Wait for response */
        String responseStr = recv(input);
        if (Objects.isNull(responseStr)) {
            throw new Exception("CONNECTION WAS CLOSED");
        }

        resp = new UnifiedMessage().deserialize(responseStr);
        logger.debug("RESPONSE <- {}:{} MessageType={}, StatusType={}",
            socket.getLocalAddress(), socket.getPort(),
            resp.getMessageType(),
            resp.getStatusType());
        logger.debug(rheader);

        switch (resp.getStatusType()) {
            case GET_ERROR:
            case PUT_ERROR:
            case DELETE_ERROR:
            case CLIENT_ERROR:
            case ERROR:
                throw new Exception("ERROR: request failed");
        }

        return resp;
    }

    public void close() {
        /*
         * Sends CLIENT_CLOSE to server. This will let server
         * know the client has closed - TCP has a session close
         * handshake. From the server-side, server would receive
         * this CLIENT_CLOSE and throw IOException or return -1
         * when input.read() is called.
         *
         * Point is, close() is all we need to call on client-
         * side.
         */
        Socket _socket;
        try {
            socket.close();
            _socket = CONNECTION_POOL.remove(getSocketName());
            assert(_socket.getPort() == port);
        } catch (Exception e) {
            logger.error("Error closing socket", e);
        }
    }

    public static boolean send(OutputStream output, String message) {
        byte[] messageBytes;
        try {
            messageBytes = ArrayUtils.addAll(
                compress(message), DEADBEEF.getBytes()
            );
        } catch (Exception e) {
            return false;
        }

        logger.debug("SEND: # Bytes = {}", messageBytes.length);
        try {
            logger.trace("SEND_MESSAGE: {}", message);
            output.write(messageBytes, 0, messageBytes.length);
            output.flush();
        } catch (Exception e) {
            logger.error("Failed to send message", e);
            return false;
        }

        return true;
    }

    public static String recv(InputStream input) {
        String response = null;

        BufferedInputStream bis;
        ByteArrayOutputStream bas;
        try {
            bis = new BufferedInputStream(input);
            bas = new ByteArrayOutputStream();
            byte[] buf = new byte[MAX_READ_BYTES];

            /*
             * read() blocks until receives transmission
             */
            int totalBytes = 0;
            int len;
            byte[] lastNBytes = new byte[0];
            while ((len = bis.read(buf, 0, MAX_READ_BYTES)) > 0) {
                int bytesLeft = bis.available();
                logger.trace(
                    "RECV_READ # BYTES = {} | {} BYTES REMAINING",
                    len, bytesLeft);

                totalBytes += len;

                /*
                 * Check for DEADBEEF (message terminator). If last
                 * bytes read are 0xDEADBEEF, then all segments have
                 * been read (TCP guarantees message order, so this is
                 * true).
                 */
                boolean finished = false;
                if (isDeadbeef(buf, lastNBytes, len)) {
                    logger.trace("\"{}\": Transmission finished",
                        DEADBEEF);
                    len = len-DEADBEEF.length();
                    finished = true;
                }
                if (len > 0) {
                    bas.write(buf, 0, len);
                }

                /*
                 * InputStream.read() will block until next recv'd
                 * data. This means that if we have read ALL data in
                 * the current iteration, the next iteration will
                 * block. We have to check for this case:
                 * InputStream.available() returns the amount of data
                 * still to be read. If this is 0, then break out of
                 * loop to prevent blocking indefinitely.
                 */
                if (bytesLeft == 0 && finished) {
                    logger.debug("RECV: Total # bytes={}", totalBytes);
                    break;
                }

                lastNBytes = ArrayUtils.subarray(buf,
                    buf.length-DEADBEEF.length(), buf.length);
            }

            /*
             * If connection has closed, then either read()
             * will throw IOException, or return -1. Either
             * case return NULL to indicate connection has
             * closed.
             *
             * Since read() blocks until data is received,
             * if it returns -1, it means something bad happened
             * to the connection - we test for that here.
             *
             * Another idea is that response should NEVER be
             * an empty string (unless client deliberately
             * sent an empty string). An empty string response
             * indicates that NO data was read. Since read()
             * returned, and no data was read, it must mean
             * the underlying connection has been closed.
             */
            byte[] msgBytes = bas.toByteArray();

            /* Removing stray DEADBEEF */
            if (len < 0) {
                msgBytes = ArrayUtils.subarray(
                    msgBytes,0,msgBytes.length+len
                );
            }
            response = decompress(msgBytes);
            if (len < 0 && response.isEmpty()) {
                return null;
            }
        } catch (IOException e) {
            logger.debug("Stream closed unexpectedly", e);
            response = null;
        } catch (Exception e) {
            logger.error("RECV was incomplete", e);
            response = null;
        }

        //logger.trace("RECV_MESSAGE: {}", response);
        return response;
    }

    public static byte[] compress(String in) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream defl = new DeflaterOutputStream(out);
            defl.write(in.getBytes());
            defl.flush();
            defl.close();

            return out.toByteArray();
        } catch (Exception e) {
            logger.trace("Compression failed");
            throw e;
        }
    }

    public static String decompress(byte[] in) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream infl = new InflaterOutputStream(out);
            infl.write(in);
            infl.flush();
            infl.close();

            return new String(out.toByteArray());
        } catch (Exception e) {
            logger.trace("Decompression failed");
            throw e;
        }
    }

    private Socket connect(String host, int port, int timeout) throws Exception {
        Socket _socket;
        if (Objects.nonNull(_socket = CONNECTION_POOL.get(getSocketName()))) {
            if (_socket.isConnected()) {
                return _socket;
            }
        }

        _socket = new Socket(host, port);
        if (timeout > 0) {
            _socket.setSoTimeout(timeout);
        }

        try {
            InputStream _input = _socket.getInputStream();

            /*
             * Server must send a CONNECTION_ESTABLISHED message
             * (e.g. ACK) back to client. This is listening
             * for that message. If that message is not sent,
             * the client cannot know the server has received the
             * connection.
             */
            while (true) {
                String msg = recv(_input);

                if (Objects.nonNull(msg) && !msg.isEmpty()) {
                    logger.trace("CONNECTION ACK: {}", msg);
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("Connection error", e);
            throw e;
        }

        CONNECTION_POOL.put(getSocketName(), _socket);
        return _socket;
    }

    private String getSocketName() {
        return String.format("%s:%d", host, port);
    }

    private static boolean isDeadbeef(byte[] bytes, byte[] patchBytes, int len) {
        if (len < DEADBEEF.length()) {
            bytes = ArrayUtils.addAll(patchBytes, bytes);
            len += patchBytes.length;
        }

        String lastChars = new String(Arrays.copyOfRange(
            bytes, len-DEADBEEF.length(), len
        ));

        return lastChars.equals(DEADBEEF);
    }

    private static String format(String message) {
        if (Objects.isNull(message)) {
            return "NULL";
        }

        if (message.length() <= 200) {
            return message;
        } else {
            return String.format("%s ... %s",
                StringUtils.left(message, 60),
                StringUtils.right(message, 60)
            );
        }
    }
}
