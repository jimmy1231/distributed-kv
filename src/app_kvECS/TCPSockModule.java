package app_kvECS;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.UnifiedMessage;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class TCPSockModule {
    private static Logger logger = LoggerFactory.getLogger(TCPSockModule.class);
    private static int MAX_READ_BYTES = 4096;
    private static String DEADBEEF = "_______DEADBEEF_______";

    private InputStream input;
    private OutputStream output;
    private Socket socket;

    /****************************************************/
    public TCPSockModule(String host, int port) throws Exception {
        /* Establish socket connection */
        socket = connect(host, port);
        logger.info(String.format(
            "ECSSocket connection established: %s:%d",
            host, port));

        input = this.socket.getInputStream();
        output = this.socket.getOutputStream();
    }

    public TCPSockModule(String host, int port, int timeout) throws Exception {
        /* Establish socket connection with timeout */
        socket = connect(host, port, timeout);
        logger.debug(String.format(
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
        String rheader = StringUtils.repeat("-",25);
        logger.info(rheader);
        logger.info("REQUEST -> {}:{} MessageType={}, StatusType={}",
            socket.getLocalAddress(), socket.getPort(),
            request.getMessageType(),
            request.getStatusType());
        if (!send(output, request.serialize())) {
            logger.info("Failed to send request");
            throw new Exception("SEND failed");
        }

        /* Wait for response */
        String responseStr = recv(input);
        if (Objects.isNull(responseStr)) {
            throw new Exception("CONNECTION WAS CLOSED");
        }

        resp = new UnifiedMessage().deserialize(responseStr);
        logger.info("RESPONSE <- {}:{} MessageType={}, StatusType={}",
            socket.getLocalAddress(), socket.getPort(),
            request.getMessageType(),
            request.getStatusType());
        logger.info(rheader);
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
        try {
            socket.close();
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

        logger.info("SEND: # Bytes = {}", messageBytes.length);
        try {
            logger.debug("SEND_MESSAGE: {}", message);
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
            while ((len = bis.read(buf, 0, MAX_READ_BYTES)) > 0) {
                int bytesLeft = bis.available();
                logger.debug(
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
                if (isDeadbeef(buf, len)) {
                    logger.debug("\"{}\": Transmission finished",
                        DEADBEEF);
                    len -= DEADBEEF.length();
                    finished = true;
                }
                bas.write(buf, 0, len);

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
                    logger.info("RECV: Total # bytes={}", totalBytes);
                    break;
                }
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
            response = decompress(bas.toByteArray());
            if (len < 0 && response.isEmpty()) {
                return null;
            }
        } catch (IOException e) {
            logger.info("Stream closed unexpectedly", e);
            response = null;
        } catch (Exception e) {
            logger.error("RECV was incomplete", e);
            response = null;
        }

        logger.debug("RECV_MESSAGE: {}", response);
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
            logger.debug("Compression failed");
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
            logger.debug("Decompression failed");
            throw e;
        }
    }

    private Socket connect(String host, int port) throws Exception {
        Socket _socket = new Socket(host, port);
        return connect(_socket);
    }

    private Socket connect(String host, int port, int timeout) throws Exception {
        Socket _socket = new Socket(host, port);
        _socket.setSoTimeout(timeout);
        return connect(_socket);
    }

    private Socket connect(Socket _socket) throws Exception {
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
                    logger.debug("CONNECTION ACK: {}", msg);
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("Connection error", e);
            throw e;
        }

        return _socket;
    }

    private static boolean isDeadbeef(byte[] bytes, int len) {
        if (len < DEADBEEF.length()) {
            return false;
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
