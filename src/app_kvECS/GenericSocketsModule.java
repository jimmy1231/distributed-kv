package app_kvECS;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.log4j.Logger;
import shared.messages.UnifiedRequestResponse;

import java.io.*;
import java.net.Socket;
import java.util.Objects;

public class GenericSocketsModule {
    private static Logger logger = Logger.getLogger(GenericSocketsModule.class);
    private static int MAX_READ_BYTES = 4096;

    private InputStream input;
    private OutputStream output;
    private Socket socket;

    /****************************************************/
    public GenericSocketsModule(String host, int port) throws Exception {
        /* Establish socket connection */
        socket = connect(host, port);

        logger.info(String.format(
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
    public UnifiedRequestResponse doRequest(UnifiedRequestResponse request) throws Exception {
        UnifiedRequestResponse response = null;

        /* Do request */
        byte[] requestBytes = request.serialize().getBytes();
        output.write(requestBytes, 0, requestBytes.length);
        output.flush();

        /* Wait for response */
        while(true) {
            try {
                String responseStr = recv();
                if (Objects.nonNull(responseStr)) {
                    System.out.println(responseStr);
                    response = new UnifiedRequestResponse().deserialize(responseStr);
                    break;
                }
            } catch (Exception e) {
                System.out.println("ERRORRRRR: " + e.toString());
                logger.error("ECS do request error", e);
                throw e;
            }
        }

        return response;
    }

    public void close() {
        try {
            input.close();
            output.close();
            socket.close();
        } catch (Exception e) {
            logger.error("Error closing Sockets Module connection", e);
        }
    }

    private String recv() {
        String response = null;

        BufferedInputStream bis;
        ByteArrayOutputStream bas;
        try {
            bis = new BufferedInputStream(input);
            bas = new ByteArrayOutputStream();
            byte[] buf = new byte[MAX_READ_BYTES];

            int len;
            while ((len = bis.read(buf)) > 0) {
                bas.write(buf, 0, len);
                break;
            }
            response = bas.toString("UTF-8");
        } catch (IOException e) {
            logger.error("Error reading input stream", e);
            response = null;
        }

        return response;
    }

    public static String recv(InputStream input) {
        String response = null;

        BufferedInputStream bis;
        ByteArrayOutputStream bas;
        try {
            bis = new BufferedInputStream(input);
            bas = new ByteArrayOutputStream();
            byte[] buf = new byte[MAX_READ_BYTES];

            int len;
            while ((len = bis.read(buf)) > 0) {
                bas.write(buf, 0, len);
                logger.info("WRITING: " + bas.toString("UTF-8"));
                break;
            }
            response = bas.toString("UTF-8");
        } catch (IOException e) {
            logger.error("Error reading input stream", e);
            response = null;
        }

        return response;
    }

    private Socket connect(String host, int port) throws Exception {
        Socket _socket = null;
        try {
            _socket = new Socket(host, port);
            BufferedReader input = new BufferedReader(
                new InputStreamReader(_socket.getInputStream())
            );
            while (true) {
                String msgString = input.readLine();

                if (msgString != null) {
                    System.out.println(msgString);
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("Error initializing sockets module", e);
            throw e;
        }

        return _socket;
    }
}
